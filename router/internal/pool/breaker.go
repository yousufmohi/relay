package pool

import (
	"sync"
	"time"
)

// BreakerState is the classic three-state circuit breaker.
type BreakerState int

const (
	// Closed: traffic flows; consecutive failures are counted.
	Closed BreakerState = iota
	// Open: no traffic until the cooldown elapses.
	Open
	// HalfOpen: exactly one trial request is let through. Success closes the
	// breaker, failure re-opens it.
	HalfOpen
)

func (s BreakerState) String() string {
	return [...]string{"closed", "open", "half_open"}[s]
}

// Breaker trips after Threshold consecutive failures (errors or stalled
// streams) and probes recovery after Cooldown.
type Breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	state       BreakerState
	failures    int       // consecutive failures while closed
	openedAt    time.Time // when we last opened
	trialActive bool      // a half-open trial request is in flight
}

func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now}
}

// State returns the current state, moving Open → HalfOpen once the cooldown
// has elapsed.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advanceLocked()
	return b.state
}

// Allow reports whether a request may be sent now WITHOUT reserving anything
// (used to filter candidates before routing).
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advanceLocked()
	return b.state == Closed || (b.state == HalfOpen && !b.trialActive)
}

// Acquire is called when a request is actually routed to the worker. In the
// half-open state it claims the single trial slot.
func (b *Breaker) Acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advanceLocked()
	switch b.state {
	case Closed:
		return true
	case HalfOpen:
		if b.trialActive {
			return false
		}
		b.trialActive = true
		return true
	default:
		return false
	}
}

// Success records a successful request or health probe.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state, b.failures, b.trialActive = Closed, 0, false
}

// Failure records a failed request or probe.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advanceLocked()
	switch b.state {
	case HalfOpen:
		b.openLocked() // the trial failed: back to open
	case Closed:
		b.failures++
		if b.failures >= b.threshold {
			b.openLocked()
		}
	}
}

// Trip opens the breaker immediately (e.g. the worker is known to be gone).
func (b *Breaker) Trip() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.openLocked()
}

func (b *Breaker) openLocked() {
	b.state, b.openedAt, b.failures, b.trialActive = Open, b.now(), 0, false
}

func (b *Breaker) advanceLocked() {
	if b.state == Open && b.now().Sub(b.openedAt) >= b.cooldown {
		b.state, b.trialActive = HalfOpen, false
	}
}
