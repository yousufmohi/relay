// Package api serves the OpenAI-compatible HTTP API and runs the request
// flow:
//
//	admission → tokenize + block hashes → route → stream (with failover) → metrics
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/yousufmohi/relay/router/internal/admission"
	"github.com/yousufmohi/relay/router/internal/chat"
	"github.com/yousufmohi/relay/router/internal/config"
	"github.com/yousufmohi/relay/router/internal/failover"
	"github.com/yousufmohi/relay/router/internal/pool"
	"github.com/yousufmohi/relay/router/internal/prefix"
	"github.com/yousufmohi/relay/router/internal/routing"
	"github.com/yousufmohi/relay/router/internal/telemetry"
	"github.com/yousufmohi/relay/router/internal/tokenize"
	"github.com/yousufmohi/relay/router/internal/worker"
)

// Server holds everything the request path needs.
type Server struct {
	Cfg       config.Config
	Pool      *pool.Pool
	Policy    routing.Policy
	Admission *admission.Controller
	Tokenizer tokenize.Tokenizer
	Template  chat.Template
	Client    *worker.Client
	Metrics   *telemetry.Metrics
	Log       *slog.Logger
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.auth(s.handleChat))
	mux.HandleFunc("POST /v1/completions", s.auth(s.handleCompletions))
	mux.HandleFunc("GET /v1/models", s.auth(s.handleModels))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("GET /debug/workers", s.handleDebugWorkers)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.Metrics.Registry, promhttp.HandlerOpts{}))
	return mux
}

func (s *Server) auth(h http.HandlerFunc) http.HandlerFunc {
	if s.Cfg.APIKey == "" {
		return h
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+s.Cfg.APIKey {
			writeError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
			return
		}
		h(w, r)
	}
}

// ------------------------------------------------------------------ requests

// params are the generation settings shared by both endpoints.
type params struct {
	Model         string          `json:"model"`
	MaxTokens     *int            `json:"max_tokens"`
	MaxCompletion *int            `json:"max_completion_tokens"`
	Temperature   *float64        `json:"temperature"`
	TopP          float64         `json:"top_p"`
	Seed          *int64          `json:"seed"`
	Stop          json.RawMessage `json:"stop"`
	Stream        bool            `json:"stream"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
}

func (p params) maxTokens() int {
	switch {
	case p.MaxCompletion != nil:
		return *p.MaxCompletion
	case p.MaxTokens != nil:
		return *p.MaxTokens
	}
	return 256
}

func (p params) temperature() float64 {
	if p.Temperature == nil {
		return 1.0 // OpenAI default
	}
	return *p.Temperature
}

func (p params) stops() []string {
	if len(p.Stop) == 0 {
		return nil
	}
	var one string
	if json.Unmarshal(p.Stop, &one) == nil {
		return []string{one}
	}
	var many []string
	json.Unmarshal(p.Stop, &many)
	return many
}

type chatMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// text accepts both "content": "..." and "content": [{"type":"text","text":"..."}].
func (m chatMessage) text() string {
	var s string
	if json.Unmarshal(m.Content, &s) == nil {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	json.Unmarshal(m.Content, &parts)
	var b strings.Builder
	for _, p := range parts {
		if p.Type == "text" {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		params
		Messages []chatMessage `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "body must be JSON with a non-empty messages array")
		return
	}
	msgs := make([]chat.Message, len(req.Messages))
	for i, m := range req.Messages {
		msgs[i] = chat.Message{Role: m.Role, Content: m.text()}
	}
	s.serve(w, r, "chat", s.Template.Segments(msgs), req.params, s.Template.Stop())
}

func (s *Server) handleCompletions(w http.ResponseWriter, r *http.Request) {
	var req struct {
		params
		Prompt string `json:"prompt"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Prompt == "" {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "body must be JSON with a non-empty prompt string")
		return
	}
	s.serve(w, r, "text", []string{req.Prompt}, req.params, nil)
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": []map[string]any{
		{"id": s.modelName(""), "object": "model", "owned_by": "relay"},
	}})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if s.Pool.Healthy() == 0 {
		http.Error(w, "no eligible workers", http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ready"))
}

func (s *Server) handleDebugWorkers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"router_id": s.Cfg.RouterID, "policy": s.Policy.Name(),
		"queue_depth": s.Admission.QueueDepth(), "inflight": s.Admission.InFlight(),
		"workers": s.Pool.Snapshot(),
	})
}

func (s *Server) modelName(requested string) string {
	if s.Cfg.WorkerModel != "" {
		return s.Cfg.WorkerModel
	}
	if requested != "" {
		return requested
	}
	return "relay"
}

// --------------------------------------------------------------- core flow

func (s *Server) serve(w http.ResponseWriter, r *http.Request, kind string, segments []string, p params, templateStops []string) {
	start := time.Now()
	policy := s.Policy.Name()
	ctx, span := telemetry.Tracer.Start(r.Context(), "relay.request", trace.WithAttributes(
		attribute.String("relay.kind", kind), attribute.String("relay.policy", policy)))
	defer span.End()

	// 1. Admission control.
	deadline := start.Add(s.Cfg.QueueDeadline)
	if v, err := strconv.Atoi(r.Header.Get("X-Relay-Deadline-Ms")); err == nil && v > 0 {
		deadline = start.Add(time.Duration(v) * time.Millisecond)
	}
	actx, aspan := telemetry.Tracer.Start(ctx, "admission")
	ticket, err := s.Admission.Acquire(actx, deadline)
	aspan.End()
	if err != nil {
		var re *admission.RejectError
		if errors.As(err, &re) {
			s.Metrics.Rejected.WithLabelValues(re.Reason).Inc()
			s.Metrics.Requests.WithLabelValues(policy, strconv.Itoa(re.Status)).Inc()
			span.SetAttributes(attribute.String("relay.rejected", re.Reason))
			w.Header().Set("Retry-After", strconv.Itoa(re.RetryAfterSeconds()))
			writeError(w, re.Status, "rate_limit_exceeded", "router overloaded ("+re.Reason+"), retry later")
			return
		}
		s.Metrics.Requests.WithLabelValues(policy, "cancelled").Inc()
		return // client went away while queued
	}
	defer ticket.Release()
	span.SetAttributes(attribute.Int64("relay.queue_wait_ms", time.Since(start).Milliseconds()))

	// 2. Prefix block hashes.
	prompt := chat.Join(segments)
	toks, _ := s.Tokenizer.Tokenize(ctx, segments)
	req := routing.Request{Blocks: prefix.BlockHashes(toks, s.Cfg.BlockSize), Key: prefix.KeyOf(toks)}
	span.SetAttributes(attribute.Int("relay.prompt_blocks", len(req.Blocks)))

	// 3. Routing decision.
	dec, err := s.route(ctx, req, nil)
	if err != nil {
		s.Metrics.Requests.WithLabelValues(policy, "503").Inc()
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "no_workers", "no eligible workers")
		return
	}
	s.Metrics.MatchBlocks.WithLabelValues(policy).Observe(float64(dec.MatchBlocks))

	// 4. Stream with failover.
	sw := &streamWriter{w: w, kind: kind, stream: p.Stream, model: s.modelName(p.Model),
		id: newID(kind), created: start.Unix(), worker: dec.WorkerID}
	var firstAt, lastAt time.Time
	hooks := failover.Hooks{
		Pick: func(ctx context.Context, _ string, exclude map[string]bool) (string, error) {
			// The continuation prompt starts with the original prompt, so
			// the original block hashes are still the right routing key.
			d, err := s.route(ctx, req, exclude)
			return d.WorkerID, err
		},
		Open: s.opener(ctx, p, append(templateStops, p.stops()...)),
		Emit: func(c failover.Chunk) error {
			if c.Text == "" {
				return nil
			}
			now := time.Now()
			if firstAt.IsZero() {
				firstAt = now
				s.Metrics.TTFT.WithLabelValues(policy).Observe(now.Sub(start).Seconds())
				span.AddEvent("first_token")
			} else {
				s.Metrics.ITL.WithLabelValues(policy).Observe(now.Sub(lastAt).Seconds())
			}
			lastAt = now
			return sw.text(c.Text)
		},
		OnFailure: func(id string, err error) {
			if wk := s.Pool.Get(id); wk != nil {
				wk.Breaker.Failure()
			}
			span.AddEvent("failover", trace.WithAttributes(attribute.String("worker", id), attribute.String("error", err.Error())))
			s.Log.Warn("worker failed during request", "worker", id, "err", err)
		},
		OnSuccess: func(id string) {
			if wk := s.Pool.Get(id); wk != nil {
				wk.Breaker.Success()
			}
		},
		Retryable:  worker.Retryable,
		WorkerDown: s.Pool.IsDown,
	}
	fcfg := failover.Config{StallTimeout: s.Cfg.StallTimeout, FirstTokenTimeout: s.Cfg.FirstTokenTimeout, MaxFailovers: s.Cfg.MaxFailovers}
	res := failover.Run(ctx, fcfg,
		prompt, p.maxTokens(), dec.WorkerID, hooks)

	// 5. Metrics and the end of the response.
	s.Metrics.Failovers.Add(float64(res.Failovers))
	for _, pause := range res.Pauses {
		s.Metrics.FailoverPause.Observe(pause.Seconds())
	}
	s.Metrics.PromptTokens.WithLabelValues(policy).Add(float64(res.Usage.PromptTokens))
	s.Metrics.CachedTokens.WithLabelValues(policy).Add(float64(res.Usage.CachedTokens))
	span.SetAttributes(attribute.Int("relay.failovers", res.Failovers), attribute.Int("relay.cached_tokens", res.Usage.CachedTokens),
		attribute.Int("relay.prompt_tokens", res.Usage.PromptTokens), attribute.Int("relay.completion_tokens", res.Emitted))

	ext := relayExt{Worker: dec.WorkerID, Workers: res.Workers, Failovers: res.Failovers, MatchBlocks: dec.MatchBlocks, Policy: policy}
	for _, pause := range res.Pauses {
		ext.PauseMs = append(ext.PauseMs, float64(pause.Microseconds())/1000)
	}
	usage := usageJSON{PromptTokens: res.Usage.PromptTokens, CompletionTokens: res.Emitted,
		TotalTokens: res.Usage.PromptTokens + res.Emitted, Details: &usageDetails{CachedTokens: res.Usage.CachedTokens}}

	switch {
	case errors.Is(res.Err, failover.ErrClientGone):
		s.Metrics.Requests.WithLabelValues(policy, "cancelled").Inc()
	case res.Err != nil:
		span.SetStatus(codes.Error, res.Err.Error())
		code := http.StatusBadGateway
		var se *worker.StatusError
		if errors.As(res.Err, &se) && se.Code < 500 {
			code = se.Code
		}
		s.Metrics.Requests.WithLabelValues(policy, strconv.Itoa(code)).Inc()
		s.Log.Error("request failed", "err", res.Err, "workers", res.Workers)
		sw.fail(code, res.Err.Error(), ext)
	default:
		s.Metrics.Requests.WithLabelValues(policy, "200").Inc()
		sw.finish(res.FinishReason, usage, ext, p.StreamOptions != nil && p.StreamOptions.IncludeUsage)
	}
}

// route builds the policy's view of eligible workers, asks the policy, and
// records the blocks just sent in the chosen worker's prefix index.
func (s *Server) route(ctx context.Context, req routing.Request, exclude map[string]bool) (routing.Decision, error) {
	_, span := telemetry.Tracer.Start(ctx, "route")
	defer span.End()
	views := s.Pool.Eligible(exclude)
	if len(views) == 0 {
		return routing.Decision{}, routing.ErrNoWorkers
	}
	idx := s.Pool.Indexes()
	ws := make([]routing.Worker, len(views))
	for i, v := range views {
		ws[i] = routing.Worker{ID: v.ID, Load: v.Load, InFlight: v.InFlight, KVUsage: v.KVUsage}
		if s.Policy.NeedsMatch() {
			ws[i].MatchBlocks = idx.MatchLength(v.ID, req.Blocks)
		}
	}
	d, err := s.Policy.Pick(req, ws)
	if err != nil {
		return d, err
	}
	if !s.Policy.NeedsMatch() {
		// Baselines don't look at the index, but we still report how much
		// of the prompt their choice had cached, for comparison.
		d.MatchBlocks = idx.MatchLength(d.WorkerID, req.Blocks)
	}
	// The worker will hold these blocks once it has processed the prompt.
	idx.For(d.WorkerID).Insert(req.Blocks)

	s.Metrics.RouteExcluded.Add(float64(len(d.Excluded)))
	span.SetAttributes(attribute.String("relay.worker", d.WorkerID), attribute.Int("relay.match_blocks", d.MatchBlocks),
		attribute.Int("relay.candidates", len(ws)), attribute.StringSlice("relay.bounded_load_excluded", d.Excluded),
		attribute.String("relay.reason", d.Reason))
	if len(d.Scores) > 0 {
		b, _ := json.Marshal(d.Scores)
		span.SetAttributes(attribute.String("relay.scores", string(b)))
	}
	return d, nil
}

// opener returns the failover Open hook: claim the worker (breaker + in-flight
// accounting) and start the upstream completion.
func (s *Server) opener(reqCtx context.Context, p params, stops []string) func(context.Context, string, string, int) (failover.Stream, error) {
	return func(ctx context.Context, id, prompt string, maxTokens int) (failover.Stream, error) {
		wk := s.Pool.Get(id)
		if wk == nil {
			return nil, fmt.Errorf("worker %s left the pool", id)
		}
		if !wk.Breaker.Acquire() {
			return nil, fmt.Errorf("worker %s: circuit open", id)
		}
		_, span := telemetry.Tracer.Start(reqCtx, "upstream", trace.WithAttributes(
			attribute.String("relay.worker", id), attribute.Int("relay.max_tokens", maxTokens)))
		wk.Begin()
		st, err := s.Client.Complete(ctx, wk.URL, worker.CompletionRequest{
			Prompt: prompt, MaxTokens: maxTokens, Temperature: p.temperature(), TopP: p.TopP, Seed: p.Seed, Stop: stops,
		})
		if err != nil {
			wk.End()
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return nil, err
		}
		return &trackedStream{Stream: st, done: func() { wk.End(); span.End() }}, nil
	}
}

// trackedStream releases the worker's in-flight slot exactly once on Close.
type trackedStream struct {
	failover.Stream
	done   func()
	closed bool
}

func (t *trackedStream) Close() error {
	err := t.Stream.Close()
	if !t.closed {
		t.closed = true
		t.done()
	}
	return err
}
