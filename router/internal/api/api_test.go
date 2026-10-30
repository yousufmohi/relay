package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yousufmohi/relay/router/internal/admission"
	"github.com/yousufmohi/relay/router/internal/chat"
	"github.com/yousufmohi/relay/router/internal/config"
	"github.com/yousufmohi/relay/router/internal/pool"
	"github.com/yousufmohi/relay/router/internal/prefix"
	"github.com/yousufmohi/relay/router/internal/routing"
	"github.com/yousufmohi/relay/router/internal/telemetry"
	"github.com/yousufmohi/relay/router/internal/tokenize"
	"github.com/yousufmohi/relay/router/internal/worker"
)

var words = strings.Fields("alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima")

// greedy is a deterministic "model": the next word depends only on the text
// so far, like temperature-0 decoding.
func greedy(ctx string) string {
	if len(ctx) > 32 {
		ctx = ctx[len(ctx)-32:]
	}
	h := fnv.New32a()
	h.Write([]byte(ctx))
	return " " + words[h.Sum32()%uint32(len(words))]
}

// fakeLlama serves llama.cpp's /completion. If dieAfter > 0 it aborts the
// connection after that many tokens (a crashed worker).
func fakeLlama(t *testing.T, dieAfter int, delay time.Duration) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			return
		case "/tokenize":
			var req struct{ Content string }
			json.NewDecoder(r.Body).Decode(&req)
			json.NewEncoder(w).Encode(map[string]any{"tokens": tokenize.Bytes([]byte(req.Content))})
			return
		}
		var req struct {
			Prompt   string `json:"prompt"`
			NPredict int    `json:"n_predict"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "text/event-stream")
		text := req.Prompt
		for i := 0; i < req.NPredict; i++ {
			if dieAfter > 0 && i == dieAfter {
				panic(http.ErrAbortHandler) // drop the connection mid-stream
			}
			time.Sleep(delay)
			wd := greedy(text)
			text += wd
			b, _ := json.Marshal(map[string]any{"content": wd, "stop": false})
			fmt.Fprintf(w, "data: %s\n\n", b)
			w.(http.Flusher).Flush()
		}
		fmt.Fprintf(w, "data: {\"content\":\"\",\"stop\":true,\"stop_type\":\"limit\",\"timings\":{\"cache_n\":0,\"prompt_n\":%d}}\n\n", len(req.Prompt)/4)
	}))
}

func newTestServer(t *testing.T, policy string, perWorker, queueMax int, urls ...string) *httptest.Server {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := worker.NewClient(worker.LlamaCpp, "")
	p := pool.New(ctx, pool.Config{BlockSize: 4, HealthInterval: time.Hour, HealthTimeout: time.Second,
		SidecarTimeout: time.Second, BreakerThreshold: 3, BreakerCooldown: time.Second}, client.Health, prefix.NewIndexes(64), log)
	var eps []pool.Endpoint
	for i, u := range urls {
		eps = append(eps, pool.Endpoint{ID: string(rune('a' + i)), URL: u})
	}
	p.SetMembers(eps)
	pol, _ := routing.New(policy, routing.DefaultConfig())
	tmpl, _ := chat.Get("chatml-plain")
	adm := admission.New(admission.Config{QueueMax: queueMax, Capacity: func() int { return p.Healthy() * perWorker }})
	cfg := config.Config{BlockSize: 4, StallTimeout: time.Second, MaxFailovers: 2, QueueDeadline: 5 * time.Second}
	s := &Server{Cfg: cfg, Pool: p, Policy: pol, Admission: adm, Tokenizer: tokenize.Approx{}, Template: tmpl,
		Client: client, Metrics: telemetry.NewMetrics(p, func() float64 { return 0 }, func() float64 { return 0 }), Log: log}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv
}

type sseResult struct {
	text   string
	finish string
	relay  map[string]any
	usage  map[string]any
	done   bool
}

func chatStream(t *testing.T, url, content string, maxTokens int) (sseResult, *http.Response) {
	body := fmt.Sprintf(`{"messages":[{"role":"user","content":%q}],"max_tokens":%d,"stream":true,"stream_options":{"include_usage":true},"temperature":0}`, content, maxTokens)
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r sseResult
	if resp.StatusCode != 200 {
		return r, resp
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := strings.TrimPrefix(sc.Text(), "data: ")
		if line == "" {
			continue
		}
		if line == "[DONE]" {
			r.done = true
			break
		}
		var ev struct {
			Choices []struct {
				Delta        map[string]string `json:"delta"`
				FinishReason *string           `json:"finish_reason"`
			}
			Usage map[string]any `json:"usage"`
			Relay map[string]any `json:"relay"`
			Error map[string]any `json:"error"`
		}
		json.Unmarshal([]byte(line), &ev)
		if ev.Error != nil {
			t.Fatalf("stream error: %v", ev.Error)
		}
		if len(ev.Choices) > 0 {
			r.text += ev.Choices[0].Delta["content"]
			if ev.Choices[0].FinishReason != nil {
				r.finish = *ev.Choices[0].FinishReason
			}
		}
		if ev.Usage != nil {
			r.usage, r.relay = ev.Usage, ev.Relay
		}
	}
	return r, resp
}

// expected computes what the greedy model produces for a prompt.
func expected(prompt string, n int) string {
	text, out := prompt, ""
	for i := 0; i < n; i++ {
		w := greedy(text)
		text += w
		out += w
	}
	return out
}

func TestChatStreamEndToEnd(t *testing.T) {
	w := fakeLlama(t, 0, 0)
	defer w.Close()
	srv := newTestServer(t, "relay", 4, 10, w.URL)
	r, resp := chatStream(t, srv.URL, "hello", 6)
	if resp.StatusCode != 200 || !r.done || r.finish != "length" {
		t.Fatalf("status=%d %+v", resp.StatusCode, r)
	}
	prompt := "<|im_start|>user\nhello<|im_end|>\n<|im_start|>assistant\n"
	if r.text != expected(prompt, 6) {
		t.Fatalf("text %q want %q", r.text, expected(prompt, 6))
	}
	if r.usage["completion_tokens"].(float64) != 6 || r.relay["worker"] != "a" {
		t.Fatalf("usage=%v relay=%v", r.usage, r.relay)
	}
	if resp.Header.Get("X-Relay-Worker") != "a" {
		t.Fatal("missing X-Relay-Worker")
	}
}

func TestMidStreamFailoverOverHTTP(t *testing.T) {
	dying := fakeLlama(t, 3, 5*time.Millisecond) // dies after 3 tokens
	healthy := fakeLlama(t, 0, 5*time.Millisecond)
	defer dying.Close()
	defer healthy.Close()
	// round_robin picks worker "a" (the dying one) first.
	srv := newTestServer(t, "round_robin", 4, 10, dying.URL, healthy.URL)
	r, resp := chatStream(t, srv.URL, "tell me a story", 10)
	if resp.StatusCode != 200 || !r.done {
		t.Fatalf("client must see a complete stream, got status %d %+v", resp.StatusCode, r)
	}
	prompt := "<|im_start|>user\ntell me a story<|im_end|>\n<|im_start|>assistant\n"
	// temperature=0: the continuation on the second worker is coherent —
	// identical to an uninterrupted run.
	if want := expected(prompt, 10); r.text != want {
		t.Fatalf("incoherent continuation:\n got %q\nwant %q", r.text, want)
	}
	if r.relay["failovers"].(float64) != 1 {
		t.Fatalf("relay ext %v", r.relay)
	}
	if ws := r.relay["workers"].([]any); len(ws) != 2 || ws[0] != "a" || ws[1] != "b" {
		t.Fatalf("workers %v", ws)
	}
}

func TestAdmission429WithRetryAfter(t *testing.T) {
	slow := fakeLlama(t, 0, 50*time.Millisecond)
	defer slow.Close()
	srv := newTestServer(t, "relay", 1, 0, slow.URL) // 1 slot, no queue
	var busy atomic.Bool
	done := make(chan struct{})
	go func() {
		busy.Store(true)
		chatStream(t, srv.URL, "first", 10)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	_, resp := chatStream(t, srv.URL, "second", 2)
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("want 429 with Retry-After, got %d %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	<-done
}

func TestNonStreamingCompletion(t *testing.T) {
	w := fakeLlama(t, 0, 0)
	defer w.Close()
	srv := newTestServer(t, "least_loaded", 4, 10, w.URL)
	resp, err := http.Post(srv.URL+"/v1/completions", "application/json", strings.NewReader(`{"prompt":"abc","max_tokens":3}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Choices []struct {
			Text string `json:"text"`
		} `json:"choices"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != 200 || out.Choices[0].Text != expected("abc", 3) {
		t.Fatalf("%d %+v", resp.StatusCode, out)
	}
}

func TestAllWorkersDeadGivesCleanError(t *testing.T) {
	a, b, c := fakeLlama(t, 1, 0), fakeLlama(t, 1, 0), fakeLlama(t, 1, 0)
	defer a.Close()
	defer b.Close()
	defer c.Close()
	srv := newTestServer(t, "round_robin", 4, 10, a.URL, b.URL, c.URL)
	body := `{"messages":[{"role":"user","content":"x"}],"max_tokens":10,"stream":true}`
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	b2, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	// Tokens were already streamed, so the error arrives as an SSE event.
	if !strings.Contains(string(b2), "relay_failover_exhausted") || !strings.Contains(string(b2), "[DONE]") {
		t.Fatalf("want clean error event, got %s", b2)
	}
}
