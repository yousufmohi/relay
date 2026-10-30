// Package scrape reads load and cache numbers from a model server's
// Prometheus endpoint (llama.cpp or vLLM).
package scrape

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Sample is one scrape of the model server.
type Sample struct {
	Running, Waiting int
	KVUsage          float64 // 0..1
	CapacityTokens   uint64
	HitTokens        uint64 // cumulative prefix-cache hit tokens (0 if unknown)
	QueryTokens      uint64 // cumulative prefix-cache query tokens
	Progress         uint64 // any cumulative counter; going backwards = restart
	StartTime        float64
	Model            string
}

// Scraper fetches one Sample.
type Scraper interface {
	Scrape(ctx context.Context) (Sample, error)
}

// New returns the scraper for a flavor ("llamacpp" or "vllm").
func New(flavor, baseURL string) Scraper {
	c := &http.Client{Timeout: 2 * time.Second}
	if flavor == "vllm" {
		return &vllm{base: baseURL, c: c}
	}
	return &llamaCpp{base: baseURL, c: c}
}

// Metrics is a parsed Prometheus text exposition: name → sum over series.
// Labels are kept for the few metrics that carry data in labels.
type Metrics struct {
	Values map[string]float64
	Labels map[string]map[string]string // first series' labels per name
}

// Parse reads the Prometheus text format (enough of it for model servers:
// "name{labels} value [timestamp]" lines; comments ignored).
func Parse(r io.Reader) (Metrics, error) {
	m := Metrics{Values: map[string]float64{}, Labels: map[string]map[string]string{}}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		name, labels, rest := line, "", ""
		if i := strings.IndexByte(line, '{'); i >= 0 {
			j := strings.LastIndexByte(line, '}')
			if j < i {
				continue
			}
			name, labels, rest = line[:i], line[i+1:j], strings.TrimSpace(line[j+1:])
		} else if i := strings.IndexAny(line, " \t"); i >= 0 {
			name, rest = line[:i], strings.TrimSpace(line[i+1:])
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		m.Values[name] += v
		if _, ok := m.Labels[name]; !ok && labels != "" {
			m.Labels[name] = parseLabels(labels)
		}
	}
	return m, sc.Err()
}

func parseLabels(s string) map[string]string {
	out := map[string]string{}
	for s != "" {
		eq := strings.IndexByte(s, '=')
		if eq < 0 || eq+1 >= len(s) || s[eq+1] != '"' {
			break
		}
		key := strings.TrimSpace(s[:eq])
		s = s[eq+2:]
		end := strings.IndexByte(s, '"')
		if end < 0 {
			break
		}
		out[key] = s[:end]
		s = strings.TrimLeft(s[end+1:], ", ")
	}
	return out
}

func get(ctx context.Context, c *http.Client, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("scrape: %s: HTTP %d", url, resp.StatusCode)
	}
	return resp, nil
}

func fetchMetrics(ctx context.Context, c *http.Client, url string) (Metrics, error) {
	resp, err := get(ctx, c, url)
	if err != nil {
		return Metrics{}, err
	}
	defer resp.Body.Close()
	return Parse(resp.Body)
}

// first returns the value of the first name present.
func (m Metrics) first(names ...string) (float64, bool) {
	for _, n := range names {
		if v, ok := m.Values[n]; ok {
			return v, true
		}
	}
	return 0, false
}

// ---------------------------------------------------------------- llama.cpp

// llamaCpp scrapes `llama-server --metrics`. Capacity comes from /props.
type llamaCpp struct {
	base     string
	c        *http.Client
	capacity uint64
	model    string
}

func (l *llamaCpp) Scrape(ctx context.Context) (Sample, error) {
	m, err := fetchMetrics(ctx, l.c, l.base+"/metrics")
	if err != nil {
		l.capacity = 0 // re-read props after the server comes back
		return Sample{}, err
	}
	if l.capacity == 0 {
		l.readProps(ctx)
	}
	running, _ := m.first("llamacpp:requests_processing")
	waiting, _ := m.first("llamacpp:requests_deferred")
	kv, _ := m.first("llamacpp:kv_cache_usage_ratio")
	prompt, _ := m.first("llamacpp:prompt_tokens_total")
	pred, _ := m.first("llamacpp:tokens_predicted_total")
	hits, _ := m.first("relaysim:prefix_cache_hit_tokens_total")
	queries, _ := m.first("relaysim:prefix_cache_query_tokens_total")
	start, _ := m.first("process_start_time_seconds")
	return Sample{
		Running: int(running), Waiting: int(waiting), KVUsage: kv, CapacityTokens: l.capacity,
		HitTokens: uint64(hits), QueryTokens: uint64(queries), Progress: uint64(prompt + pred),
		StartTime: start, Model: l.model,
	}, nil
}

func (l *llamaCpp) readProps(ctx context.Context) {
	resp, err := get(ctx, l.c, l.base+"/props")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var p struct {
		TotalSlots int    `json:"total_slots"`
		ModelPath  string `json:"model_path"`
		Settings   struct {
			NCtx int `json:"n_ctx"`
		} `json:"default_generation_settings"`
		SimCache int `json:"relay_sim_cache_tokens"`
	}
	if json.NewDecoder(resp.Body).Decode(&p) != nil {
		return
	}
	l.model = p.ModelPath
	switch {
	case p.SimCache > 0:
		l.capacity = uint64(p.SimCache)
	case p.Settings.NCtx > 0:
		// n_ctx here is per slot; the whole context is shared by all slots.
		l.capacity = uint64(p.Settings.NCtx * max(1, p.TotalSlots))
	}
}

// --------------------------------------------------------------------- vLLM

// vllm scrapes vLLM's /metrics (V1 names first, V0 names as fallback).
type vllm struct {
	base string
	c    *http.Client
}

func (v *vllm) Scrape(ctx context.Context) (Sample, error) {
	m, err := fetchMetrics(ctx, v.c, v.base+"/metrics")
	if err != nil {
		return Sample{}, err
	}
	running, _ := m.first("vllm:num_requests_running")
	waiting, _ := m.first("vllm:num_requests_waiting")
	kv, _ := m.first("vllm:kv_cache_usage_perc", "vllm:gpu_cache_usage_perc")
	hits, _ := m.first("vllm:prefix_cache_hits_total", "vllm:gpu_prefix_cache_hits_total")
	queries, _ := m.first("vllm:prefix_cache_queries_total", "vllm:gpu_prefix_cache_queries_total")
	prompt, _ := m.first("vllm:prompt_tokens_total")
	gen, _ := m.first("vllm:generation_tokens_total")
	start, _ := m.first("process_start_time_seconds")
	s := Sample{
		Running: int(running), Waiting: int(waiting), KVUsage: kv,
		HitTokens: uint64(hits), QueryTokens: uint64(queries), Progress: uint64(prompt + gen), StartTime: start,
	}
	if l := m.Labels["vllm:cache_config_info"]; l != nil {
		blocks, _ := strconv.ParseUint(l["num_gpu_blocks"], 10, 64)
		size, _ := strconv.ParseUint(l["block_size"], 10, 64)
		s.CapacityTokens = blocks * size
	}
	for _, labels := range m.Labels {
		if name := labels["model_name"]; name != "" {
			s.Model = name
			break
		}
	}
	return s, nil
}
