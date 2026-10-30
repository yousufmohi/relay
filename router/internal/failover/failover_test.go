package failover

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeWorker deterministically "generates" the next word from the context:
// word i of a fixed sentence, where i = number of words already in the
// prompt after the original prompt. That mimics temperature=0 decoding: the
// continuation depends only on the text so far, so a failover must produce
// exactly the same final text as an uninterrupted run.
var sentence = strings.Fields("the quick brown fox jumps over the lazy dog and keeps running far away")

type behaviour struct {
	failAfter int           // break the stream after this many tokens (-1 = never)
	hangAfter int           // stop sending (stall) after this many tokens (-1 = never)
	openErr   error         // fail to open
	openHang  bool          // never return from Open (until ctx cancelled)
	delay     time.Duration // per token
}

type fakeCluster struct {
	mu        sync.Mutex
	base      string
	workers   map[string]behaviour
	opened    []string
	prompts   []string
	maxTokens []int
	closed    int
}

func (f *fakeCluster) open(ctx context.Context, id, prompt string, maxTokens int) (Stream, error) {
	f.mu.Lock()
	b := f.workers[id]
	f.opened = append(f.opened, id)
	f.prompts = append(f.prompts, prompt)
	f.maxTokens = append(f.maxTokens, maxTokens)
	f.mu.Unlock()
	if b.openErr != nil {
		return nil, b.openErr
	}
	if b.openHang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := len(strings.Fields(strings.TrimPrefix(prompt, f.base)))
	return &fakeStream{f: f, ctx: ctx, b: b, pos: done, left: maxTokens}, nil
}

type fakeStream struct {
	f        *fakeCluster
	ctx      context.Context
	b        behaviour
	pos      int // next word index
	left     int // tokens left to produce
	sent     int
	finished bool
}

func (s *fakeStream) Recv() (Chunk, error) {
	if s.finished {
		return Chunk{}, io.EOF
	}
	if s.b.hangAfter >= 0 && s.sent >= s.b.hangAfter {
		<-s.ctx.Done()
		return Chunk{}, s.ctx.Err()
	}
	if s.b.failAfter >= 0 && s.sent >= s.b.failAfter {
		return Chunk{}, io.ErrUnexpectedEOF
	}
	if s.b.delay > 0 {
		time.Sleep(s.b.delay)
	}
	if s.left == 0 || s.pos >= len(sentence) {
		s.finished = true
		fr := "stop"
		if s.left == 0 {
			fr = "length"
		}
		return Chunk{FinishReason: fr, Usage: &Usage{PromptTokens: 10, CachedTokens: 4}}, nil
	}
	w := " " + sentence[s.pos]
	s.pos++
	s.left--
	s.sent++
	return Chunk{Text: w, Tokens: 1}, nil
}

func (s *fakeStream) Close() error {
	s.f.mu.Lock()
	s.f.closed++
	s.f.mu.Unlock()
	return nil
}

func never() behaviour { return behaviour{failAfter: -1, hangAfter: -1} }

type harness struct {
	f        *fakeCluster
	emitted  []Chunk
	failures []string
	order    []string // pick order for failovers
	hooks    Hooks
}

func newHarness(workers map[string]behaviour, order ...string) *harness {
	h := &harness{f: &fakeCluster{base: "PROMPT:", workers: workers}, order: order}
	h.hooks = Hooks{
		Pick: func(_ context.Context, _ string, exclude map[string]bool) (string, error) {
			for _, id := range h.order {
				if !exclude[id] {
					return id, nil
				}
			}
			return "", errors.New("no workers")
		},
		Open: h.f.open,
		Emit: func(c Chunk) error { h.emitted = append(h.emitted, c); return nil },
		OnFailure: func(id string, _ error) {
			h.failures = append(h.failures, id)
		},
	}
	return h
}

func (h *harness) text() string {
	var b strings.Builder
	for _, c := range h.emitted {
		b.WriteString(c.Text)
	}
	return b.String()
}

var cfg = Config{StallTimeout: 100 * time.Millisecond, MaxFailovers: 2}

func TestNoFailure(t *testing.T) {
	h := newHarness(map[string]behaviour{"a": never()})
	r := Run(context.Background(), cfg, "PROMPT:", 5, "a", h.hooks)
	if r.Err != nil || r.Failovers != 0 || r.Emitted != 5 || r.FinishReason != "length" {
		t.Fatalf("%+v", r)
	}
	if r.Text != " the quick brown fox jumps" || h.text() != r.Text {
		t.Fatalf("text %q / %q", r.Text, h.text())
	}
	if r.Usage.CachedTokens != 4 {
		t.Fatalf("usage not captured: %+v", r.Usage)
	}
}

func TestFailoverMidStreamIsSeamless(t *testing.T) {
	// Reference run without failure.
	ref := Run(context.Background(), cfg, "PROMPT:", 8, "a", newHarness(map[string]behaviour{"a": never()}).hooks)

	b := never()
	b.failAfter = 3
	h := newHarness(map[string]behaviour{"a": b, "b": never()}, "a", "b")
	r := Run(context.Background(), cfg, "PROMPT:", 8, "a", h.hooks)
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	// temperature=0 analogue: the client sees exactly the same text.
	if r.Text != ref.Text || h.text() != ref.Text {
		t.Fatalf("continuation differs:\n got %q\nwant %q", r.Text, ref.Text)
	}
	if r.Failovers != 1 || len(r.Pauses) != 1 || r.Emitted != 8 {
		t.Fatalf("%+v", r)
	}
	if len(h.failures) != 1 || h.failures[0] != "a" {
		t.Fatalf("failure not reported: %v", h.failures)
	}
	// The retry must carry the generated text and the reduced budget.
	if h.f.prompts[1] != "PROMPT: the quick brown" || h.f.maxTokens[1] != 5 {
		t.Fatalf("retry prompt %q max %d", h.f.prompts[1], h.f.maxTokens[1])
	}
	if got := r.Workers; len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("workers %v", got)
	}
}

func TestStallTriggersFailover(t *testing.T) {
	b := never()
	b.hangAfter = 2
	h := newHarness(map[string]behaviour{"a": b, "b": never()}, "a", "b")
	start := time.Now()
	r := Run(context.Background(), cfg, "PROMPT:", 6, "a", h.hooks)
	if r.Err != nil || r.Failovers != 1 || r.Emitted != 6 {
		t.Fatalf("%+v", r)
	}
	if r.Pauses[0] < cfg.StallTimeout {
		t.Fatalf("pause %v shorter than stall timeout", r.Pauses[0])
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("took too long")
	}
}

func TestOpenErrorFailsOverBeforeFirstToken(t *testing.T) {
	h := newHarness(map[string]behaviour{"a": {openErr: errors.New("connection refused")}, "b": never()}, "a", "b")
	r := Run(context.Background(), cfg, "PROMPT:", 3, "a", h.hooks)
	if r.Err != nil || r.Failovers != 1 || r.Text != " the quick brown" {
		t.Fatalf("%+v", r)
	}
}

func TestOpenHangCountsAsStall(t *testing.T) {
	h := newHarness(map[string]behaviour{"a": {openHang: true}, "b": never()}, "a", "b")
	r := Run(context.Background(), cfg, "PROMPT:", 2, "a", h.hooks)
	if r.Err != nil || r.Failovers != 1 {
		t.Fatalf("%+v", r)
	}
}

func TestMaxFailoversThenCleanError(t *testing.T) {
	bad := never()
	bad.failAfter = 1
	h := newHarness(map[string]behaviour{"a": bad, "b": bad, "c": bad, "d": never()}, "a", "b", "c", "d")
	r := Run(context.Background(), cfg, "PROMPT:", 10, "a", h.hooks)
	if !errors.Is(r.Err, ErrExhausted) {
		t.Fatalf("want ErrExhausted, got %v", r.Err)
	}
	if r.Failovers != 2 || len(h.f.opened) != 3 {
		t.Fatalf("failovers=%d opened=%v", r.Failovers, h.f.opened)
	}
	if r.Emitted != 3 { // one token from each of a, b, c
		t.Fatalf("emitted %d", r.Emitted)
	}
}

func TestFailedWorkersAreExcluded(t *testing.T) {
	bad := never()
	bad.failAfter = 1
	h := newHarness(map[string]behaviour{"a": bad, "b": bad, "c": never()}, "a", "b", "c")
	r := Run(context.Background(), cfg, "PROMPT:", 5, "a", h.hooks)
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if got := strings.Join(h.f.opened, ","); got != "a,b,c" {
		t.Fatalf("each worker tried once in order, got %s", got)
	}
}

func TestNoWorkerForFailover(t *testing.T) {
	bad := never()
	bad.failAfter = 1
	h := newHarness(map[string]behaviour{"a": bad}, "a")
	r := Run(context.Background(), cfg, "PROMPT:", 5, "a", h.hooks)
	if !errors.Is(r.Err, ErrExhausted) {
		t.Fatalf("got %v", r.Err)
	}
}

func TestNonRetryableErrorStops(t *testing.T) {
	fatal := errors.New("400 prompt too long")
	h := newHarness(map[string]behaviour{"a": {openErr: fatal}, "b": never()}, "a", "b")
	h.hooks.Retryable = func(err error) bool { return !errors.Is(err, fatal) }
	r := Run(context.Background(), cfg, "PROMPT:", 5, "a", h.hooks)
	if !errors.Is(r.Err, fatal) || len(h.f.opened) != 1 {
		t.Fatalf("err=%v opened=%v", r.Err, h.f.opened)
	}
}

func TestClientGoneStopsWithoutBlamingWorker(t *testing.T) {
	h := newHarness(map[string]behaviour{"a": never()}, "a")
	n := 0
	h.hooks.Emit = func(Chunk) error {
		n++
		if n == 2 {
			return errors.New("broken pipe")
		}
		return nil
	}
	r := Run(context.Background(), cfg, "PROMPT:", 5, "a", h.hooks)
	if !errors.Is(r.Err, ErrClientGone) || len(h.failures) != 0 {
		t.Fatalf("err=%v failures=%v", r.Err, h.failures)
	}
	if h.f.closed != 1 {
		t.Fatalf("upstream must be closed, closed=%d", h.f.closed)
	}
}

func TestContextCancelStops(t *testing.T) {
	b := never()
	b.delay = 20 * time.Millisecond
	h := newHarness(map[string]behaviour{"a": b}, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	r := Run(ctx, cfg, "PROMPT:", 100, "a", h.hooks)
	if !errors.Is(r.Err, ErrClientGone) || r.Failovers != 0 {
		t.Fatalf("%+v", r)
	}
}

func TestFailureAfterBudgetReachedFinishesWithLength(t *testing.T) {
	b := never()
	b.failAfter = 3 // breaks right after emitting the 3rd (=last allowed) token
	h := newHarness(map[string]behaviour{"a": b, "b": never()}, "a", "b")
	r := Run(context.Background(), cfg, "PROMPT:", 3, "a", h.hooks)
	if r.Err != nil || r.FinishReason != "length" || r.Failovers != 0 {
		t.Fatalf("%+v", r)
	}
}

func TestSuccessHookCalled(t *testing.T) {
	h := newHarness(map[string]behaviour{"a": never()}, "a")
	var ok []string
	h.hooks.OnSuccess = func(id string) { ok = append(ok, id) }
	Run(context.Background(), cfg, "PROMPT:", 2, "a", h.hooks)
	if len(ok) != 1 || ok[0] != "a" {
		t.Fatalf("%v", ok)
	}
}

func TestSlowFirstTokenIsNotAStall(t *testing.T) {
	// A busy worker queues the request for longer than StallTimeout before
	// the first token. That is load, not failure: no failover.
	b := never()
	b.delay = 150 * time.Millisecond // > StallTimeout (100ms), < FirstTokenTimeout
	h := newHarness(map[string]behaviour{"a": b, "b": never()}, "a", "b")
	c := Config{StallTimeout: 400 * time.Millisecond, FirstTokenTimeout: time.Second, MaxFailovers: 2}
	c.StallTimeout = 100 * time.Millisecond
	// With delay on every token the stall timer would fire after the first
	// token, so emit just one token.
	r := Run(context.Background(), c, "PROMPT:", 1, "a", h.hooks)
	if r.Err != nil || r.Failovers != 0 {
		t.Fatalf("%+v", r)
	}
}

func TestWorkerDownAbortsPromptly(t *testing.T) {
	b := never()
	b.hangAfter = 1
	h := newHarness(map[string]behaviour{"a": b, "b": never()}, "a", "b")
	var down atomic.Bool
	h.hooks.WorkerDown = func(id string) bool { return id == "a" && down.Load() }
	go func() { time.Sleep(50 * time.Millisecond); down.Store(true) }()
	c := Config{StallTimeout: 10 * time.Second, FirstTokenTimeout: 10 * time.Second, MaxFailovers: 2}
	start := time.Now()
	r := Run(context.Background(), c, "PROMPT:", 4, "a", h.hooks)
	if r.Err != nil || r.Failovers != 1 || time.Since(start) > 2*time.Second {
		t.Fatalf("%+v in %v", r, time.Since(start))
	}
}
