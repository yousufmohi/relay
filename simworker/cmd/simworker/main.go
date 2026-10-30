// Command simworker emulates a llama.cpp server with a prefix KV cache, so
// routing experiments can run with many workers on a laptop in minutes.
//
// It speaks the subset of llama.cpp's HTTP API Relay uses (/completion with
// SSE streaming, /tokenize, /health, /metrics, /props, /slots) and models the
// costs that matter for routing:
//
//   - a fixed number of slots (concurrent sequences); extra requests queue;
//   - prefill time proportional to the prompt tokens NOT found in the prefix
//     cache, slowed down by concurrent work;
//   - per-token decode time that grows with batch size;
//   - an LRU prefix cache of chained 16-token block hashes with finite
//     capacity, so cache-unaware routing thrashes it.
//
// Output text is a deterministic function of the context (a greedy-decoding
// stand-in): the same prompt always yields the same continuation. That lets
// tests check that failover continuations are coherent.
//
// Fault injection (for experiments): POST /admin/fault?mode=hang|crash|none.
// "hang" freezes every stream and stops answering (a network partition as
// seen from outside); "crash" exits the process.
package main

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	addr         = flag.String("addr", ":8080", "listen address")
	slots        = flag.Int("slots", 4, "concurrent sequences")
	cacheTokens  = flag.Int("cache-tokens", 32768, "prefix cache capacity (tokens)")
	prefillUs    = flag.Float64("prefill-us-per-token", 400, "prefill cost per uncached token (microseconds)")
	decodeMs     = flag.Float64("decode-ms", 20, "decode step time with one sequence (ms)")
	batchPenalty = flag.Float64("batch-penalty", 0.15, "decode slowdown per extra concurrent sequence")
	model        = flag.String("model", "sim-qwen2.5-7b", "model name reported")
	blockTokens  = 16
)

func main() {
	flag.Parse()
	s := newServer()
	mux := http.NewServeMux()
	mux.HandleFunc("/completion", s.completion)
	mux.HandleFunc("/tokenize", s.tokenize)
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/props", s.props)
	mux.HandleFunc("/slots", s.slotsInfo)
	mux.HandleFunc("/admin/fault", s.fault)
	log.Printf("simworker on %s slots=%d cache=%d tokens", *addr, *slots, *cacheTokens)
	log.Fatal(http.ListenAndServe(*addr, s.gate(mux)))
}

type server struct {
	sem   chan struct{} // slot semaphore
	cache *blockCache

	running, waiting atomic.Int64
	runningTokens    atomic.Int64 // tokens held by running sequences (KV usage)
	promptTotal      atomic.Int64
	predictedTotal   atomic.Int64
	hitTokens        atomic.Int64
	queryTokens      atomic.Int64
	hang             atomic.Bool
	hangMu           sync.Mutex
	hangCh           chan struct{}
	start            time.Time
}

func newServer() *server {
	return &server{sem: make(chan struct{}, *slots), cache: newBlockCache(*cacheTokens / blockTokens),
		hangCh: make(chan struct{}), start: time.Now()}
}

// gate implements the "hang" fault: requests block forever (until the client
// gives up), like a partitioned host.
func (s *server) gate(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.hang.Load() && r.URL.Path != "/admin/fault" {
			s.hangMu.Lock()
			ch := s.hangCh
			s.hangMu.Unlock()
			select {
			case <-r.Context().Done():
			case <-ch:
			}
			return
		}
		h.ServeHTTP(w, r)
	})
}

func (s *server) fault(w http.ResponseWriter, r *http.Request) {
	s.hangMu.Lock()
	defer s.hangMu.Unlock()
	switch r.URL.Query().Get("mode") {
	case "hang":
		if s.hang.CompareAndSwap(false, true) {
			s.hangCh = make(chan struct{})
		}
	case "crash":
		log.Print("fault: crash")
		os.Exit(1)
	default:
		if s.hang.CompareAndSwap(true, false) {
			close(s.hangCh)
		}
	}
	fmt.Fprintln(w, "ok")
}

// tokens: 4 bytes per token (same scheme as the router's approx tokenizer).
func toTokens(s string) []int32 {
	b := []byte(s)
	out := make([]int32, 0, (len(b)+3)/4)
	for i := 0; i < len(b); i += 4 {
		var v uint32
		for j := 0; j < 4 && i+j < len(b); j++ {
			v |= uint32(b[i+j]) << (8 * j)
		}
		out = append(out, int32(v))
	}
	return out
}

func blockHashes(toks []int32) []uint64 {
	n := len(toks) / blockTokens
	out := make([]uint64, n)
	var prev uint64
	buf := make([]byte, 4)
	for i := 0; i < n; i++ {
		h := fnv.New64a()
		var pb [8]byte
		binary.LittleEndian.PutUint64(pb[:], prev)
		h.Write(pb[:])
		for _, t := range toks[i*blockTokens : (i+1)*blockTokens] {
			binary.LittleEndian.PutUint32(buf, uint32(t))
			h.Write(buf)
		}
		prev = h.Sum64()
		out[i] = prev
	}
	return out
}

func (s *server) tokenize(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Content string `json:"content"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	json.NewEncoder(w).Encode(map[string]any{"tokens": toTokens(req.Content)})
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprint(w, `{"status":"ok"}`)
}

func (s *server) props(w http.ResponseWriter, _ *http.Request) {
	json.NewEncoder(w).Encode(map[string]any{
		"total_slots":                 *slots,
		"model_path":                  *model,
		"default_generation_settings": map[string]any{"n_ctx": *cacheTokens / *slots},
		"relay_sim_cache_tokens":      *cacheTokens,
	})
}

func (s *server) slotsInfo(w http.ResponseWriter, _ *http.Request) {
	out := make([]map[string]any, *slots)
	busy := int(s.running.Load())
	for i := range out {
		out[i] = map[string]any{"id": i, "n_ctx": *cacheTokens / *slots, "is_processing": i < busy}
	}
	json.NewEncoder(w).Encode(out)
}

// metrics uses llama.cpp's metric names plus relaysim:* counters for the
// prefix cache (llama.cpp has no such counter; vLLM does).
func (s *server) metrics(w http.ResponseWriter, _ *http.Request) {
	usage := float64(s.runningTokens.Load()) / float64(*cacheTokens)
	fmt.Fprintf(w, "# TYPE llamacpp:requests_processing gauge\nllamacpp:requests_processing %d\n", s.running.Load())
	fmt.Fprintf(w, "# TYPE llamacpp:requests_deferred gauge\nllamacpp:requests_deferred %d\n", s.waiting.Load())
	fmt.Fprintf(w, "# TYPE llamacpp:kv_cache_usage_ratio gauge\nllamacpp:kv_cache_usage_ratio %.4f\n", math.Min(usage, 1))
	fmt.Fprintf(w, "# TYPE llamacpp:prompt_tokens_total counter\nllamacpp:prompt_tokens_total %d\n", s.promptTotal.Load())
	fmt.Fprintf(w, "# TYPE llamacpp:tokens_predicted_total counter\nllamacpp:tokens_predicted_total %d\n", s.predictedTotal.Load())
	fmt.Fprintf(w, "# TYPE relaysim:prefix_cache_hit_tokens_total counter\nrelaysim:prefix_cache_hit_tokens_total %d\n", s.hitTokens.Load())
	fmt.Fprintf(w, "# TYPE relaysim:prefix_cache_query_tokens_total counter\nrelaysim:prefix_cache_query_tokens_total %d\n", s.queryTokens.Load())
	fmt.Fprintf(w, "# TYPE relaysim:cache_capacity_tokens gauge\nrelaysim:cache_capacity_tokens %d\n", *cacheTokens)
	fmt.Fprintf(w, "# TYPE process_start_time_seconds gauge\nprocess_start_time_seconds %d\n", s.start.Unix())
}

var vocab = strings.Fields(`the a of to and in is that for it as with was on be by this are or from at
which an have not has but can more one all were their will there been its also into than other some
when these only time new may first such would about most two like over many use well after between
system cache token model request worker router prefix latency stream serve batch memory compute`)

// nextWord is a deterministic function of the last few bytes of context.
func nextWord(ctx string) string {
	tail := ctx
	if len(tail) > 64 {
		tail = tail[len(tail)-64:]
	}
	h := fnv.New32a()
	h.Write([]byte(tail))
	return " " + vocab[h.Sum32()%uint32(len(vocab))]
}

func (s *server) completion(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Prompt   string   `json:"prompt"`
		NPredict int      `json:"n_predict"`
		Stream   bool     `json:"stream"`
		Stop     []string `json:"stop"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.NPredict <= 0 {
		req.NPredict = 128
	}
	ctx := r.Context()

	// Wait for a slot.
	s.waiting.Add(1)
	select {
	case s.sem <- struct{}{}:
		s.waiting.Add(-1)
	case <-ctx.Done():
		s.waiting.Add(-1)
		return
	}
	s.running.Add(1)
	defer func() { <-s.sem; s.running.Add(-1) }()

	toks := toTokens(req.Prompt)
	hashes := blockHashes(toks)
	cachedBlocks := s.cache.matchAndInsert(hashes)
	cached := cachedBlocks * blockTokens
	uncached := len(toks) - cached
	s.promptTotal.Add(int64(len(toks)))
	s.hitTokens.Add(int64(cached))
	s.queryTokens.Add(int64(len(toks)))
	held := int64(len(toks))
	s.runningTokens.Add(held)
	defer func() { s.runningTokens.Add(-held) }()

	fl, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)

	// Prefill: uncached tokens cost compute, shared with concurrent work.
	contention := 1 + *batchPenalty*float64(s.running.Load()-1)
	if !sleep(r, time.Duration(float64(uncached)**prefillUs*contention)*time.Microsecond, s) {
		return
	}

	text := req.Prompt
	stopType := "limit"
	n := 0
	for n < req.NPredict {
		step := time.Duration(*decodeMs * (1 + *batchPenalty*float64(s.running.Load()-1)) * float64(time.Millisecond))
		if !sleep(r, step, s) {
			return
		}
		word := nextWord(text)
		// End naturally at a sentence boundary sometimes (deterministically).
		if n > 8 && strings.HasSuffix(word, "stream") && n%3 == 0 {
			stopType = "eos"
			break
		}
		text += word
		n++
		s.predictedTotal.Add(1)
		s.runningTokens.Add(1)
		held++
		b, _ := json.Marshal(map[string]any{"content": word, "stop": false})
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return
		}
		if fl != nil {
			fl.Flush()
		}
	}
	// Generated tokens become part of the cached sequence too.
	s.cache.matchAndInsert(blockHashes(toTokens(text)))
	final := map[string]any{
		"content": "", "stop": true, "stop_type": stopType, "tokens_predicted": n,
		"tokens_evaluated": len(toks), "timings": map[string]any{"cache_n": cached, "prompt_n": uncached},
	}
	b, _ := json.Marshal(final)
	fmt.Fprintf(w, "data: %s\n\n", b)
	if fl != nil {
		fl.Flush()
	}
}

// sleep waits d unless the client leaves or a "hang" fault freezes us.
func sleep(r *http.Request, d time.Duration, s *server) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-r.Context().Done():
		return false
	}
	if s.hang.Load() { // partitioned: stop producing tokens
		<-r.Context().Done()
		return false
	}
	return true
}

// blockCache is an LRU set of block hashes with a capacity in blocks.
type blockCache struct {
	mu    sync.Mutex
	cap   int
	order *list.List
	items map[uint64]*list.Element
}

func newBlockCache(capacity int) *blockCache {
	return &blockCache{cap: capacity, order: list.New(), items: map[uint64]*list.Element{}}
}

// matchAndInsert returns the number of leading blocks already cached and
// then marks every block as most recently used.
func (c *blockCache) matchAndInsert(hs []uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	match := 0
	for _, h := range hs {
		if _, ok := c.items[h]; !ok {
			break
		}
		match++
	}
	for i := len(hs) - 1; i >= 0; i-- {
		if el, ok := c.items[hs[i]]; ok {
			c.order.MoveToFront(el)
		} else {
			c.items[hs[i]] = c.order.PushFront(hs[i])
		}
	}
	for len(c.items) > c.cap {
		el := c.order.Back()
		c.order.Remove(el)
		delete(c.items, el.Value.(uint64))
	}
	return match
}
