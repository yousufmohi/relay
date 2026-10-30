// Command loadgen drives a workload against Relay (open-loop Poisson
// arrivals) and writes one JSON line per request.
//
//	loadgen -targets http://localhost:8080 -workload experiments/workloads/unique.yaml -out results/x.jsonl
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yousufmohi/relay/loadgen/internal/client"
	"github.com/yousufmohi/relay/loadgen/internal/workload"
)

func main() {
	var (
		targets  = flag.String("targets", "http://localhost:8080", "comma-separated router URLs")
		wlPath   = flag.String("workload", "", "workload YAML file")
		rate     = flag.Float64("rate", 0, "override request rate (req/s)")
		duration = flag.Duration("duration", 0, "override duration")
		maxTok   = flag.Int("max-tokens", 0, "override max_tokens")
		seed     = flag.Int64("seed", 1, "random seed")
		out      = flag.String("out", "", "results JSONL path (default stdout summary only)")
		apiKey   = flag.String("api-key", os.Getenv("RELAY_API_KEY"), "API key")
		maxOut   = flag.Int("max-outstanding", 4000, "cap on concurrent requests (protects the generator)")
		drainFor = flag.Duration("drain-timeout", 120*time.Second, "how long to wait for in-flight requests at the end")
		scale    = flag.Float64("rate-scale", 1, "multiply every rate (incl. schedule steps)")
	)
	flag.Parse()
	spec, err := workload.Load(*wlPath)
	if err != nil {
		log.Fatal(err)
	}
	if *rate > 0 {
		spec.Rate, spec.Schedule = *rate, nil
	}
	if *duration > 0 {
		spec.Duration.Duration = *duration
	}
	spec.Rate *= *scale
	for i := range spec.Schedule {
		spec.Schedule[i][1] *= *scale
	}
	if *maxTok > 0 {
		spec.MaxTokens = *maxTok
	}
	gen, err := workload.NewGenerator(spec, *seed)
	if err != nil {
		log.Fatal(err)
	}
	c := client.New(strings.Split(*targets, ","))
	c.APIKey, c.MaxTokens, c.Temperature = *apiKey, spec.MaxTokens, spec.Temperature

	var w *bufio.Writer
	if *out != "" {
		os.MkdirAll(filepath.Dir(*out), 0o755)
		f, err := os.Create(*out)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		w = bufio.NewWriter(f)
		defer w.Flush()
	}
	var mu sync.Mutex
	var recs []client.Record
	emit := func(r client.Record) {
		mu.Lock()
		defer mu.Unlock()
		recs = append(recs, r)
		if w != nil {
			b, _ := json.Marshal(r)
			w.Write(append(b, '\n'))
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	// Requests get a hard upper bound so a stuck stream can't hang the run.
	reqCtx, cancelReqs := context.WithCancel(context.Background())
	defer cancelReqs()

	log.Printf("workload=%s kind=%s rate=%.2f/s duration=%s max_tokens=%d targets=%s",
		spec.Name, spec.Kind, spec.Rate, spec.Duration, spec.MaxTokens, *targets)
	sem := make(chan struct{}, *maxOut)
	var wg sync.WaitGroup
	arrivals := rand.New(rand.NewSource(*seed + 7))
	runStart := time.Now()
	dropped := 0

	// Open loop: conversations arrive as a Poisson process whose rate is the
	// request rate divided by the mean number of turns per conversation.
	for {
		el := time.Since(runStart)
		if el >= spec.Duration.Duration || ctx.Err() != nil {
			break
		}
		convRate := spec.RateAt(el) / gen.AvgTurns()
		if convRate <= 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		gap := time.Duration(arrivals.ExpFloat64() / convRate * float64(time.Second))
		next := runStart.Add(el + gap)
		time.Sleep(time.Until(next))
		if time.Since(runStart) >= spec.Duration.Duration {
			break
		}
		conv := gen.Next()
		select {
		case sem <- struct{}{}:
		default:
			dropped++ // generator saturated: count instead of distorting arrivals
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			runConversation(reqCtx, c, runStart, spec.Name, conv, emit)
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(*drainFor):
		log.Printf("drain timeout: cancelling in-flight requests")
		cancelReqs()
		<-done
	}
	summarize(recs, time.Since(runStart), dropped)
}

func runConversation(ctx context.Context, c *client.Client, runStart time.Time, name string, conv workload.Conversation, emit func(client.Record)) {
	var msgs []workload.Message
	if conv.System != "" {
		msgs = append(msgs, workload.Message{Role: "system", Content: conv.System})
	}
	for i, u := range conv.UserTurns {
		msgs = append(msgs, workload.Message{Role: "user", Content: u})
		rec := client.Record{Workload: name, Conv: conv.ID, Turn: i + 1, PrefixID: conv.PrefixID}
		c.Do(ctx, runStart, msgs, &rec)
		emit(rec)
		if rec.Status != "ok" {
			return // a failed turn ends the conversation
		}
		msgs = append(msgs, workload.Message{Role: "assistant", Content: rec.Text})
		if i < len(conv.UserTurns)-1 && conv.Think > 0 {
			select {
			case <-time.After(conv.Think):
			case <-ctx.Done():
				return
			}
		}
	}
}

func summarize(recs []client.Record, el time.Duration, dropped int) {
	var ttft []float64
	ok, prompt, cached, failovers := 0, 0, 0, 0
	status := map[string]int{}
	for _, r := range recs {
		status[r.Status]++
		if r.Status == "ok" {
			ok++
			ttft = append(ttft, r.TTFTMs)
			prompt += r.PromptToks
			cached += r.CachedToks
		}
		failovers += r.Failovers
	}
	sort.Float64s(ttft)
	pct := func(p float64) float64 {
		if len(ttft) == 0 {
			return math.NaN()
		}
		return ttft[min(len(ttft)-1, int(p*float64(len(ttft))))]
	}
	hit := 0.0
	if prompt > 0 {
		hit = float64(cached) / float64(prompt)
	}
	fmt.Printf("requests=%d ok=%d status=%v dropped_by_generator=%d failovers=%d\n", len(recs), ok, status, dropped, failovers)
	fmt.Printf("throughput=%.2f req/s  ttft_p50=%.0fms ttft_p99=%.0fms  cache_hit_rate=%.3f\n",
		float64(ok)/el.Seconds(), pct(0.5), pct(0.99), hit)
}
