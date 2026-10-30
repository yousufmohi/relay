package internal

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Source reads the two signals the controller scales on.
type Source interface {
	// Read returns total queued requests and TTFT p95 over the recent window.
	Read(ctx context.Context) (queue float64, p95 time.Duration, err error)
}

// ---------------------------------------------------------------- Prometheus

// Prometheus queries a Prometheus server (the production path).
type Prometheus struct {
	URL    string
	Window string // e.g. "1m"
	HTTP   *http.Client
}

func (p *Prometheus) query(ctx context.Context, q string) (float64, error) {
	u := strings.TrimRight(p.URL, "/") + "/api/v1/query?query=" + url.QueryEscape(q)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := p.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if out.Status != "success" {
		return 0, fmt.Errorf("prometheus: query failed: %s", q)
	}
	if len(out.Data.Result) == 0 {
		return 0, nil
	}
	s, _ := out.Data.Result[0].Value[1].(string)
	v, err := strconv.ParseFloat(s, 64)
	if math.IsNaN(v) {
		v = 0
	}
	return v, err
}

func (p *Prometheus) Read(ctx context.Context) (float64, time.Duration, error) {
	// Every router reports every worker's queue; take the max per worker so
	// replicas aren't double counted, then add the routers' own queues.
	q, err := p.query(ctx, `sum(max by (worker) (relay_worker_queue_depth)) + sum(relay_queue_depth)`)
	if err != nil {
		return 0, 0, err
	}
	t, err := p.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, sum by (le) (rate(relay_ttft_seconds_bucket[%s])))`, p.Window))
	if err != nil {
		return 0, 0, err
	}
	return q, time.Duration(t * float64(time.Second)), nil
}

// -------------------------------------------------------------- direct scrape

// RouterScrape scrapes router /metrics endpoints directly and computes the
// same two signals itself (used locally, where there is no Prometheus).
type RouterScrape struct {
	URLs   []string
	Window time.Duration
	HTTP   *http.Client

	mu      sync.Mutex
	history []bucketSnap // cumulative TTFT histogram over time
}

type bucketSnap struct {
	at      time.Time
	buckets map[float64]float64 // le -> cumulative count (summed over routers)
}

func (r *RouterScrape) Read(ctx context.Context) (float64, time.Duration, error) {
	perWorker := map[string]float64{}
	queue := 0.0
	snap := bucketSnap{at: time.Now(), buckets: map[float64]float64{}}
	ok := 0
	for _, u := range r.URLs {
		samples, err := r.scrape(ctx, u)
		if err != nil {
			continue // a dead router replica must not stop scaling
		}
		ok++
		for _, s := range samples {
			switch s.name {
			case "relay_worker_queue_depth":
				perWorker[s.labels["worker"]] = math.Max(perWorker[s.labels["worker"]], s.value)
			case "relay_queue_depth":
				queue += s.value
			case "relay_ttft_seconds_bucket":
				le, err := strconv.ParseFloat(s.labels["le"], 64)
				if s.labels["le"] == "+Inf" {
					le, err = math.Inf(1), nil
				}
				if err == nil {
					snap.buckets[le] += s.value
				}
			}
		}
	}
	if ok == 0 {
		return 0, 0, fmt.Errorf("autoscaler: no router reachable")
	}
	for _, v := range perWorker {
		queue += v
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.history = append(r.history, snap)
	// Keep one snapshot older than the window as the baseline.
	for len(r.history) > 2 && snap.at.Sub(r.history[1].at) > r.Window {
		r.history = r.history[1:]
	}
	return queue, quantile(0.95, r.history[0].buckets, snap.buckets), nil
}

// quantile estimates a quantile from the increase of a cumulative histogram
// between two snapshots, interpolating linearly inside the bucket (the same
// method as PromQL's histogram_quantile).
func quantile(q float64, old, cur map[float64]float64) time.Duration {
	les := make([]float64, 0, len(cur))
	for le := range cur {
		les = append(les, le)
	}
	sort.Float64s(les)
	if len(les) == 0 {
		return 0
	}
	delta := func(le float64) float64 { return math.Max(0, cur[le]-old[le]) }
	total := delta(les[len(les)-1])
	if total <= 0 {
		return 0
	}
	rank := q * total
	prevLE, prevCount := 0.0, 0.0
	for _, le := range les {
		c := delta(le)
		if c >= rank {
			if math.IsInf(le, 1) {
				return time.Duration(prevLE * float64(time.Second))
			}
			frac := 0.0
			if c > prevCount {
				frac = (rank - prevCount) / (c - prevCount)
			}
			return time.Duration((prevLE + (le-prevLE)*frac) * float64(time.Second))
		}
		prevLE, prevCount = le, c
	}
	return time.Duration(prevLE * float64(time.Second))
}

type sample struct {
	name   string
	labels map[string]string
	value  float64
}

func (r *RouterScrape) scrape(ctx context.Context, u string) ([]sample, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(u, "/")+"/metrics", nil)
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return parseText(resp.Body)
}

func parseText(rd io.Reader) ([]sample, error) {
	var out []sample
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		s := sample{labels: map[string]string{}}
		rest := line
		if i := strings.IndexByte(line, '{'); i >= 0 {
			j := strings.LastIndexByte(line, '}')
			s.name = line[:i]
			for _, kv := range strings.Split(line[i+1:j], ",") {
				if k, v, ok := strings.Cut(kv, "="); ok {
					s.labels[strings.TrimSpace(k)] = strings.Trim(v, `"`)
				}
			}
			rest = strings.TrimSpace(line[j+1:])
		} else {
			f := strings.Fields(line)
			if len(f) < 2 {
				continue
			}
			s.name, rest = f[0], f[1]
		}
		f := strings.Fields(rest)
		if len(f) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(f[0], 64)
		if err != nil {
			continue
		}
		s.value = v
		out = append(out, s)
	}
	return out, sc.Err()
}

// ------------------------------------------------------------------ scalers

// Scaler reads and sets the worker replica count.
type Scaler interface {
	Current(ctx context.Context) (int, error)
	Scale(ctx context.Context, replicas int) error
}

// FileScaler is the local-harness backend: the controller writes the desired
// count to Path; the harness adds/drains workers and writes the actual count
// to Path+".current".
type FileScaler struct{ Path string }

func (f FileScaler) Current(context.Context) (int, error) {
	b, err := os.ReadFile(f.Path + ".current")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(b)))
}

func (f FileScaler) Scale(_ context.Context, n int) error {
	tmp := f.Path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(n)+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.Path)
}
