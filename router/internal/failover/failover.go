// Package failover streams one completion to the client and keeps it alive
// when the worker serving it dies or stalls.
//
// The trick that makes mid-stream recovery possible: the router always holds
// the full prompt and every token it has already forwarded to the client. If
// the worker fails, the router asks a DIFFERENT worker to complete
//
//	prompt + generated_so_far
//
// with max_tokens reduced by what was already emitted, and keeps writing to
// the same client connection. The client sees a pause, never an error.
//
// With sampling (temperature > 0) the continuation differs from what the dead
// worker would have produced; it is still a valid sample conditioned on the
// text so far. With temperature = 0 the continuation is the greedy
// continuation of the same context, so the output stays coherent.
package failover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// Chunk is one piece of an upstream token stream.
type Chunk struct {
	Text         string // newly generated text ("" for metadata-only chunks)
	Tokens       int    // tokens represented by Text (usually 1)
	FinishReason string // "stop" or "length" on the final content chunk
	Usage        *Usage // set on the chunk that carries usage numbers
}

// Usage is what the worker reported about the prompt it processed.
type Usage struct {
	PromptTokens int // total prompt tokens
	CachedTokens int // of which served from the prefix cache
}

// Stream is an open upstream completion stream. Recv returns io.EOF after the
// stream ends cleanly; any other error means the stream broke.
type Stream interface {
	Recv() (Chunk, error)
	Close() error
}

// Hooks connect the failover loop to the rest of the router. Keeping them as
// plain functions lets the tests drive the loop with fake workers.
type Hooks struct {
	// Pick chooses a worker for a retry, never one in exclude.
	Pick func(ctx context.Context, prompt string, exclude map[string]bool) (string, error)
	// Open starts a completion on a worker. ctx is cancelled to abort it.
	Open func(ctx context.Context, workerID, prompt string, maxTokens int) (Stream, error)
	// Emit forwards a chunk to the client. An error means the client is gone.
	Emit func(Chunk) error
	// OnFailure / OnSuccess feed the worker's circuit breaker. Optional.
	OnFailure func(workerID string, err error)
	OnSuccess func(workerID string)
	// Retryable reports whether an upstream error is worth failing over for
	// (a 400 "prompt too long" would fail on every worker). Optional;
	// default: every error is retryable.
	Retryable func(error) bool
	// WorkerDown reports whether the pool now considers the worker dead
	// (failed health checks, silent sidecar). Polled while streaming so a
	// partitioned worker is abandoned as soon as it is detected, without
	// waiting for a timeout. Optional.
	WorkerDown func(workerID string) bool
}

// Config bounds the recovery behaviour.
type Config struct {
	// StallTimeout: no token for this long AFTER the first one = stalled.
	StallTimeout time.Duration
	// FirstTokenTimeout bounds the wait for the first chunk. It is much
	// longer than StallTimeout because a healthy but busy worker sends
	// nothing while the request is queued or prefilling; that is load, not
	// failure. Zero means "same as StallTimeout".
	FirstTokenTimeout time.Duration
	MaxFailovers      int // retries after the first attempt
}

// Result summarises what happened.
type Result struct {
	Text         string          // everything emitted to the client
	Emitted      int             // tokens emitted
	FinishReason string          // "stop" | "length" | "" on error
	Usage        Usage           // from the FIRST attempt (the routing decision we evaluate)
	Workers      []string        // every worker tried, in order
	Failovers    int             // number of retries performed
	Pauses       []time.Duration // per failover: last token before → first token after
	Err          error
}

var (
	// ErrStalled is reported when no chunk arrives within the timeout.
	ErrStalled = errors.New("failover: worker stalled")
	// ErrWorkerDown is reported when the pool marks the worker dead mid-stream.
	ErrWorkerDown = errors.New("failover: worker marked down")
	// ErrClientGone is returned when Emit fails.
	ErrClientGone = errors.New("failover: client disconnected")
	// ErrExhausted wraps the last error once MaxFailovers is used up.
	ErrExhausted = errors.New("failover: retries exhausted")
)

// Run streams a completion of prompt (at most maxTokens tokens), starting on
// firstWorker and failing over as needed.
func Run(ctx context.Context, cfg Config, prompt string, maxTokens int, firstWorker string, h Hooks) Result {
	var (
		res        Result
		generated  strings.Builder
		worker     = firstWorker
		exclude    = map[string]bool{}
		lastEmit   = time.Now() // time of the last chunk forwarded to the client
		failedAt   time.Time    // set while we are recovering from a failure
		usageTaken bool
	)

	for attempt := 0; ; attempt++ {
		res.Workers = append(res.Workers, worker)
		remaining := maxTokens - res.Emitted

		// The continuation prompt is the original prompt plus everything the
		// client has already received.
		outcome := streamOne(ctx, cfg, h, worker, prompt+generated.String(), remaining, func(c Chunk) error {
			if c.Usage != nil && !usageTaken {
				res.Usage, usageTaken = *c.Usage, true
			}
			if c.Text == "" && c.FinishReason == "" {
				return nil // metadata only
			}
			if !failedAt.IsZero() && c.Text != "" {
				// First token after a failover: record the client-visible gap.
				res.Pauses = append(res.Pauses, time.Since(lastEmit))
				failedAt = time.Time{}
			}
			if err := h.Emit(c); err != nil {
				return ErrClientGone
			}
			generated.WriteString(c.Text)
			res.Emitted += c.Tokens
			if c.Text != "" {
				lastEmit = time.Now()
			}
			if c.FinishReason != "" {
				res.FinishReason = c.FinishReason
			}
			return nil
		})

		switch {
		case outcome == nil:
			// Clean end of stream.
			if h.OnSuccess != nil {
				h.OnSuccess(worker)
			}
			if res.FinishReason == "" {
				res.FinishReason = "stop"
			}
			res.Text = generated.String()
			return res
		case errors.Is(outcome, ErrClientGone) || ctx.Err() != nil:
			// The client left; nothing to recover. Not the worker's fault.
			res.Text, res.Err = generated.String(), ErrClientGone
			return res
		}

		// The worker failed (error, broken stream or stall).
		if h.OnFailure != nil {
			h.OnFailure(worker, outcome)
		}
		if h.Retryable != nil && !h.Retryable(outcome) {
			res.Text, res.Err = generated.String(), outcome
			return res
		}
		if res.FinishReason != "" {
			// Broke after the final content chunk (e.g. while sending usage):
			// the client already has the whole answer.
			res.Text = generated.String()
			return res
		}
		if res.Emitted >= maxTokens {
			res.Text, res.FinishReason = generated.String(), "length"
			return res
		}
		if res.Failovers >= cfg.MaxFailovers {
			res.Text = generated.String()
			res.Err = fmt.Errorf("%w after %d failovers: %v", ErrExhausted, res.Failovers, outcome)
			return res
		}

		// Fail over: pick another worker, never one that already failed us.
		exclude[worker] = true
		next, err := h.Pick(ctx, prompt+generated.String(), exclude)
		if err != nil {
			res.Text = generated.String()
			res.Err = fmt.Errorf("%w: no worker for failover: %v (last error: %v)", ErrExhausted, err, outcome)
			return res
		}
		res.Failovers++
		if failedAt.IsZero() {
			failedAt = time.Now()
		}
		worker = next
	}
}

// streamOne runs a single attempt: open the stream and forward chunks until
// it ends. It returns nil on a clean end, ErrClientGone if the client went
// away, ErrStalled on a stall, or the upstream error.
func streamOne(ctx context.Context, cfg Config, h Hooks, worker, prompt string, maxTokens int, forward func(Chunk) error) error {
	actx, cancel := context.WithCancel(ctx)
	defer cancel() // aborts the upstream request on every exit path

	type recv struct {
		c   Chunk
		err error
	}
	// Opening the stream counts against the stall timer too: a worker that
	// accepts the connection but never answers is as dead as one that stops
	// mid-stream.
	type openResult struct {
		s   Stream
		err error
	}
	opened := make(chan openResult, 1)
	go func() {
		s, err := h.Open(actx, worker, prompt, maxTokens)
		opened <- openResult{s, err}
	}()
	// closeLate closes a stream that finishes opening after we gave up on it.
	closeLate := func() {
		go func() {
			if r := <-opened; r.s != nil {
				r.s.Close()
			}
		}()
	}
	firstTimeout := cfg.FirstTokenTimeout
	if firstTimeout <= 0 {
		firstTimeout = cfg.StallTimeout
	}
	timer := time.NewTimer(firstTimeout)
	defer timer.Stop()
	// down fires periodically to ask the pool whether the worker died.
	var down <-chan time.Time
	if h.WorkerDown != nil {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		down = t.C
	}
	var stream Stream
wait:
	for {
		select {
		case r := <-opened:
			if r.err != nil {
				return r.err
			}
			stream = r.s
			break wait
		case <-timer.C:
			closeLate()
			return ErrStalled
		case <-down:
			if h.WorkerDown(worker) {
				closeLate()
				return ErrWorkerDown
			}
		case <-ctx.Done():
			closeLate()
			return ctx.Err()
		}
	}
	defer stream.Close()

	// Recv blocks, so read in a goroutine and select on it with a timer.
	ch := make(chan recv)
	go func() {
		for {
			c, err := stream.Recv()
			select {
			case ch <- recv{c, err}:
			case <-actx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	// The open already consumed part of the first-token budget; keep the
	// same timer running until the first chunk, then use StallTimeout.
	for {
		select {
		case r := <-ch:
			if r.err == io.EOF {
				return nil
			}
			if r.err != nil {
				return r.err
			}
			if err := forward(r.c); err != nil {
				return err
			}
			if !timer.Stop() {
				select { // drain a fired timer before Reset
				case <-timer.C:
				default:
				}
			}
			timer.Reset(cfg.StallTimeout)
		case <-timer.C:
			return ErrStalled
		case <-down:
			if h.WorkerDown(worker) {
				return ErrWorkerDown
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
