// Package admission bounds how much work the router lets through.
//
// At most Capacity() requests are dispatched to workers at once (healthy
// workers × per-worker concurrency). Beyond that, requests wait in a bounded
// FIFO queue. A request is rejected with 429 + Retry-After:
//
//   - immediately, if the queue is full;
//   - immediately, if its estimated wait already exceeds its deadline
//     (rejecting early is kinder than timing out late — the client can retry
//     elsewhere or back off right away);
//   - when its deadline passes while still queued.
package admission

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// RejectError explains why a request was not admitted.
type RejectError struct {
	Status     int           // 429 (overloaded) or 503 (no capacity at all)
	RetryAfter time.Duration // hint for the Retry-After header
	Reason     string        // "queue_full" | "deadline" | "timeout" | "no_capacity"
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("admission: rejected (%s), retry after %s", e.Reason, e.RetryAfter)
}

// RetryAfterSeconds rounds the hint up to whole seconds (min 1), as the
// Retry-After header requires.
func (e *RejectError) RetryAfterSeconds() int {
	return max(1, int(math.Ceil(e.RetryAfter.Seconds())))
}

// Config for the controller.
type Config struct {
	QueueMax int        // max waiting requests
	Capacity func() int // current dispatch limit; re-read on every decision
	// InitialService seeds the service-time estimate before any request
	// has completed.
	InitialService time.Duration
}

type waiter struct {
	ready chan struct{} // closed when a slot is handed to this waiter
}

// Controller is the admission gate. Safe for concurrent use.
type Controller struct {
	cfg Config
	now func() time.Time

	mu       sync.Mutex
	inflight int
	queue    *list.List    // of *waiter, FIFO
	service  time.Duration // EWMA of how long a request holds a slot
}

func New(cfg Config) *Controller {
	if cfg.InitialService <= 0 {
		cfg.InitialService = time.Second
	}
	return &Controller{cfg: cfg, now: time.Now, queue: list.New(), service: cfg.InitialService}
}

// Ticket is held while a request is dispatched. Release it exactly once.
type Ticket struct {
	c     *Controller
	start time.Time
	once  sync.Once
}

// Release frees the slot and records how long it was held.
func (t *Ticket) Release() {
	t.once.Do(func() { t.c.release(t.c.now().Sub(t.start)) })
}

// Acquire admits a request or returns a *RejectError. deadline is the latest
// time the request may START being served.
func (c *Controller) Acquire(ctx context.Context, deadline time.Time) (*Ticket, error) {
	c.mu.Lock()
	capacity := c.cfg.Capacity()

	// Fast path: free capacity and nobody queued ahead of us (FIFO fairness).
	if c.inflight < capacity && c.queue.Len() == 0 {
		c.inflight++
		c.mu.Unlock()
		return &Ticket{c: c, start: c.now()}, nil
	}
	if capacity <= 0 {
		c.mu.Unlock()
		return nil, &RejectError{Status: 503, RetryAfter: time.Second, Reason: "no_capacity"}
	}
	if c.queue.Len() >= c.cfg.QueueMax {
		wait := c.estimateLocked(c.queue.Len(), capacity)
		c.mu.Unlock()
		return nil, &RejectError{Status: 429, RetryAfter: wait, Reason: "queue_full"}
	}
	// Would we wait past our deadline? Then say so now.
	wait := c.estimateLocked(c.queue.Len(), capacity)
	if c.now().Add(wait).After(deadline) {
		c.mu.Unlock()
		return nil, &RejectError{Status: 429, RetryAfter: wait, Reason: "deadline"}
	}

	w := &waiter{ready: make(chan struct{})}
	el := c.queue.PushBack(w)
	c.mu.Unlock()

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-w.ready:
		return &Ticket{c: c, start: c.now()}, nil
	case <-timer.C:
	case <-ctx.Done():
	}

	// Timed out or cancelled. We might have been handed a slot concurrently;
	// if so, give it back.
	c.mu.Lock()
	select {
	case <-w.ready:
		c.mu.Unlock()
		c.release(0)
	default:
		c.queue.Remove(el)
		c.mu.Unlock()
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, &RejectError{Status: 429, RetryAfter: c.EstimatedWait(), Reason: "timeout"}
}

// estimateLocked: a request with `ahead` requests queued in front of it
// waits for ahead+1 slots to free up; slots free at about capacity per
// service time.
func (c *Controller) estimateLocked(ahead, capacity int) time.Duration {
	return time.Duration(float64(c.service) * float64(ahead+1) / float64(capacity))
}

// EstimatedWait is the expected wait for a request arriving now.
func (c *Controller) EstimatedWait() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.estimateLocked(c.queue.Len(), max(1, c.cfg.Capacity()))
}

func (c *Controller) release(held time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if held > 0 {
		// EWMA with α=0.1: smooth, but adapts within a few dozen requests.
		c.service = time.Duration(0.9*float64(c.service) + 0.1*float64(held))
	}
	c.inflight--
	c.dispatchLocked()
}

// Kick re-checks capacity (call when workers join or recover).
func (c *Controller) Kick() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dispatchLocked()
}

// dispatchLocked hands free slots to queued waiters in FIFO order.
func (c *Controller) dispatchLocked() {
	capacity := c.cfg.Capacity()
	for c.inflight < capacity && c.queue.Len() > 0 {
		w := c.queue.Remove(c.queue.Front()).(*waiter)
		c.inflight++
		close(w.ready)
	}
}

// QueueDepth returns the number of waiting requests.
func (c *Controller) QueueDepth() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queue.Len()
}

// InFlight returns the number of dispatched requests.
func (c *Controller) InFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inflight
}
