package admission

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func ctl(capacity, queue int) (*Controller, *atomic.Int64) {
	var c atomic.Int64
	c.Store(int64(capacity))
	return New(Config{QueueMax: queue, Capacity: func() int { return int(c.Load()) }, InitialService: 100 * time.Millisecond}), &c
}

func far() time.Time { return time.Now().Add(time.Hour) }

func TestAdmitsUpToCapacity(t *testing.T) {
	c, _ := ctl(2, 10)
	t1, err1 := c.Acquire(context.Background(), far())
	t2, err2 := c.Acquire(context.Background(), far())
	if err1 != nil || err2 != nil || c.InFlight() != 2 {
		t.Fatal(err1, err2)
	}
	t1.Release()
	t1.Release() // idempotent
	if c.InFlight() != 1 {
		t.Fatalf("inflight %d", c.InFlight())
	}
	t2.Release()
}

func TestQueueFullRejects429(t *testing.T) {
	c, _ := ctl(1, 1)
	c.Acquire(context.Background(), far())
	go c.Acquire(context.Background(), far()) // fills the single queue slot
	time.Sleep(20 * time.Millisecond)
	_, err := c.Acquire(context.Background(), far())
	var re *RejectError
	if !errors.As(err, &re) || re.Status != 429 || re.Reason != "queue_full" {
		t.Fatalf("got %v", err)
	}
	if re.RetryAfterSeconds() < 1 {
		t.Fatal("Retry-After must be >= 1s")
	}
}

func TestRejectsEarlyWhenDeadlineCannotBeMet(t *testing.T) {
	c, _ := ctl(1, 100)
	c.Acquire(context.Background(), far())
	// Service time estimate is 100ms; a 10ms deadline can't be met.
	start := time.Now()
	_, err := c.Acquire(context.Background(), time.Now().Add(10*time.Millisecond))
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != "deadline" {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > 5*time.Millisecond {
		t.Fatal("early rejection must not wait")
	}
}

func TestQueuedRequestGetsSlotFIFO(t *testing.T) {
	c, _ := ctl(1, 10)
	first, _ := c.Acquire(context.Background(), far())
	order := make(chan int, 2)
	for i := 1; i <= 2; i++ {
		go func(i int) {
			tk, err := c.Acquire(context.Background(), far())
			if err != nil {
				t.Error(err)
				return
			}
			order <- i
			time.Sleep(10 * time.Millisecond)
			tk.Release()
		}(i)
		time.Sleep(10 * time.Millisecond) // ensure queue order 1, 2
	}
	if c.QueueDepth() != 2 {
		t.Fatalf("queue depth %d", c.QueueDepth())
	}
	first.Release()
	if a, b := <-order, <-order; a != 1 || b != 2 {
		t.Fatalf("not FIFO: %d %d", a, b)
	}
}

func TestQueuedTimesOut(t *testing.T) {
	c, _ := ctl(1, 10)
	c.cfg.InitialService = time.Millisecond
	c.service = time.Millisecond // estimate says we'd make it…
	c.Acquire(context.Background(), far())
	_, err := c.Acquire(context.Background(), time.Now().Add(30*time.Millisecond)) // …but nobody releases
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != "timeout" {
		t.Fatalf("got %v", err)
	}
	if c.QueueDepth() != 0 {
		t.Fatal("timed-out waiter must leave the queue")
	}
}

func TestCancelledWhileQueued(t *testing.T) {
	c, _ := ctl(1, 10)
	c.Acquire(context.Background(), far())
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	if _, err := c.Acquire(ctx, far()); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if c.QueueDepth() != 0 || c.InFlight() != 1 {
		t.Fatalf("queue %d inflight %d", c.QueueDepth(), c.InFlight())
	}
}

func TestNoCapacity503(t *testing.T) {
	c, _ := ctl(0, 10)
	_, err := c.Acquire(context.Background(), far())
	var re *RejectError
	if !errors.As(err, &re) || re.Status != 503 {
		t.Fatalf("got %v", err)
	}
}

func TestKickAfterCapacityGrows(t *testing.T) {
	c, capacity := ctl(1, 10)
	c.Acquire(context.Background(), far())
	got := make(chan error, 1)
	go func() { _, err := c.Acquire(context.Background(), far()); got <- err }()
	time.Sleep(10 * time.Millisecond)
	capacity.Store(2)
	c.Kick()
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter not dispatched after capacity grew")
	}
}

func TestServiceTimeEWMA(t *testing.T) {
	c, _ := ctl(1, 10)
	before := c.EstimatedWait()
	for i := 0; i < 30; i++ {
		tk, _ := c.Acquire(context.Background(), far())
		tk.start = time.Now().Add(-time.Second) // pretend it took 1s
		tk.Release()
	}
	if c.EstimatedWait() <= before*5 {
		t.Fatalf("estimate should grow toward 1s: before %v after %v", before, c.EstimatedWait())
	}
}
