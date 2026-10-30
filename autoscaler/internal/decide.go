// Package internal holds the autoscaler's decision logic, metric sources and
// scaling backends.
package internal

import (
	"fmt"
	"math"
	"time"
)

// Targets configure the controller.
type Targets struct {
	QueuePerWorker float64       // scale up above this many queued requests per worker
	TTFTP95        time.Duration // ...or above this TTFT p95
	Min, Max       int
	UpWindow       time.Duration // overload must last this long before scaling up
	UpCooldown     time.Duration // minimum time between scale-ups
	DownWindow     time.Duration // underload must last this long before scaling down
}

// Observation is one reading of cluster state.
type Observation struct {
	Now        time.Time
	Replicas   int
	QueueDepth float64       // total queued requests (router queues + worker queues)
	TTFTP95    time.Duration // 0 = no traffic in the window
}

// Decision is the controller's output.
type Decision struct {
	Desired   int
	Direction string // "up" | "down" | "none"
	Reason    string
}

// Decider is a small state machine: it remembers how long the cluster has
// been over/under target so it only reacts to SUSTAINED conditions, scales
// up fast (proportionally) and down slowly (one replica at a time, after a
// longer window, relying on sidecar draining for safety).
type Decider struct {
	T          Targets
	aboveSince time.Time
	belowSince time.Time
	lastUp     time.Time
	lastDown   time.Time
}

func (d *Decider) Decide(o Observation) Decision {
	r := o.Replicas
	// Bounds first: they override everything.
	if r < d.T.Min {
		return Decision{Desired: d.T.Min, Direction: "up", Reason: fmt.Sprintf("below min replicas (%d < %d)", r, d.T.Min)}
	}
	if r > d.T.Max {
		return Decision{Desired: d.T.Max, Direction: "down", Reason: fmt.Sprintf("above max replicas (%d > %d)", r, d.T.Max)}
	}

	qpw := o.QueueDepth / float64(max(1, r))
	qRatio := qpw / d.T.QueuePerWorker
	tRatio := 0.0
	if o.TTFTP95 > 0 {
		tRatio = float64(o.TTFTP95) / float64(d.T.TTFTP95)
	}
	ratio := math.Max(qRatio, tRatio)
	desc := fmt.Sprintf("queue/worker=%.2f (target %.1f), ttft_p95=%s (target %s)",
		qpw, d.T.QueuePerWorker, o.TTFTP95.Round(time.Millisecond), d.T.TTFTP95)

	switch {
	case ratio > 1: // overloaded
		d.belowSince = time.Time{}
		if d.aboveSince.IsZero() {
			d.aboveSince = o.Now
		}
		if o.Now.Sub(d.aboveSince) < d.T.UpWindow {
			return Decision{Desired: r, Direction: "none", Reason: "overloaded, waiting for sustained window: " + desc}
		}
		if o.Now.Sub(d.lastUp) < d.T.UpCooldown {
			return Decision{Desired: r, Direction: "none", Reason: "overloaded, in scale-up cooldown: " + desc}
		}
		if r >= d.T.Max {
			return Decision{Desired: r, Direction: "none", Reason: "overloaded but at max replicas: " + desc}
		}
		// Proportional step, at least +1, at most doubling per decision.
		want := int(math.Ceil(float64(r) * math.Min(ratio, 2)))
		want = min(d.T.Max, max(r+1, want))
		d.lastUp, d.aboveSince = o.Now, time.Time{}
		return Decision{Desired: want, Direction: "up", Reason: "sustained overload: " + desc}

	case ratio < 0.5: // comfortably underloaded
		d.aboveSince = time.Time{}
		if d.belowSince.IsZero() {
			d.belowSince = o.Now
		}
		if r <= d.T.Min {
			return Decision{Desired: r, Direction: "none", Reason: "underloaded at min replicas: " + desc}
		}
		if o.Now.Sub(d.belowSince) < d.T.DownWindow || o.Now.Sub(d.lastDown) < d.T.DownWindow || o.Now.Sub(d.lastUp) < d.T.DownWindow {
			return Decision{Desired: r, Direction: "none", Reason: "underloaded, waiting for cooldown: " + desc}
		}
		d.lastDown, d.belowSince = o.Now, time.Time{}
		return Decision{Desired: r - 1, Direction: "down", Reason: "sustained underload: " + desc}

	default:
		d.aboveSince, d.belowSince = time.Time{}, time.Time{}
		return Decision{Desired: r, Direction: "none", Reason: "within targets: " + desc}
	}
}
