package pool

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }
func newTestBreaker(c *clock) *Breaker {
	b := NewBreaker(3, time.Second)
	b.now = c.now
	return b
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newTestBreaker(c)
	b.Failure()
	b.Failure()
	b.Success() // resets the streak
	b.Failure()
	b.Failure()
	if b.State() != Closed {
		t.Fatal("non-consecutive failures must not open")
	}
	b.Failure()
	if b.State() != Open || b.Allow() || b.Acquire() {
		t.Fatal("3 consecutive failures must open")
	}
}

func TestBreakerHalfOpenSingleTrial(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newTestBreaker(c)
	b.Trip()
	c.add(999 * time.Millisecond)
	if b.Allow() {
		t.Fatal("still cooling down")
	}
	c.add(time.Millisecond)
	if b.State() != HalfOpen || !b.Allow() {
		t.Fatal("cooldown elapsed: half-open")
	}
	if !b.Acquire() {
		t.Fatal("first trial allowed")
	}
	if b.Acquire() || b.Allow() {
		t.Fatal("only one trial at a time")
	}
	b.Success()
	if b.State() != Closed || !b.Acquire() {
		t.Fatal("successful trial closes")
	}
}

func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newTestBreaker(c)
	b.Trip()
	c.add(time.Second)
	b.Acquire()
	b.Failure()
	if b.State() != Open {
		t.Fatal("failed trial re-opens")
	}
	c.add(time.Second)
	if b.State() != HalfOpen {
		t.Fatal("and probes again after another cooldown")
	}
}
