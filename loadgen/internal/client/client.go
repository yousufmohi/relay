// Package client sends streaming chat requests and measures each one.
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yousufmohi/relay/loadgen/internal/workload"
)

// Record is one request's measurements (one JSON line in the results file).
type Record struct {
	Workload    string    `json:"workload"`
	Conv        int       `json:"conv"`
	Turn        int       `json:"turn"`
	PrefixID    int       `json:"prefix_id"`
	Start       float64   `json:"t_start"` // seconds since run start
	TTFTMs      float64   `json:"ttft_ms"` // -1 if no token
	E2EMs       float64   `json:"e2e_ms"`
	ITLMs       []float64 `json:"itl_ms,omitempty"`
	Tokens      int       `json:"tokens"`
	Status      string    `json:"status"` // ok | http_429 | http_5xx | error
	HTTPStatus  int       `json:"http_status"`
	Error       string    `json:"error,omitempty"`
	Failovers   int       `json:"failovers"`
	PauseMs     []float64 `json:"pause_ms,omitempty"`
	PromptToks  int       `json:"prompt_tokens"`
	CachedToks  int       `json:"cached_tokens"`
	Worker      string    `json:"worker"`
	MatchBlocks int       `json:"match_blocks"`
	Retries429  int       `json:"retries_429"`
	ConnRetries int       `json:"conn_retries"`
	Target      string    `json:"target"`
	Text        string    `json:"-"`
}

// Client spreads requests over router targets and retries transport
// failures on the next target (that is how clients ride out a router
// replica dying) and 429s after Retry-After.
type Client struct {
	Targets     []string
	APIKey      string
	Model       string
	MaxTokens   int
	Temperature float64
	Max429      int // max Retry-After retries
	MaxConn     int // max transport-failure retries
	KeepText    bool
	HTTP        *http.Client
	next        atomic.Uint64
}

func New(targets []string) *Client {
	return &Client{Targets: targets, Model: "relay", MaxTokens: 64, Max429: 3, MaxConn: 3,
		HTTP: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1024, DisableCompression: true}}}
}

type attemptResult struct {
	status    int
	retryAt   time.Duration
	transport bool // connection-level failure: retry elsewhere
	err       error
}

// Do sends one chat request and fills rec. runStart anchors rec.Start.
func (c *Client) Do(ctx context.Context, runStart time.Time, msgs []workload.Message, rec *Record) {
	start := time.Now()
	rec.Start = start.Sub(runStart).Seconds()
	rec.TTFTMs = -1
	body, _ := json.Marshal(map[string]any{
		"model": c.Model, "messages": msgs, "max_tokens": c.MaxTokens, "temperature": c.Temperature,
		"stream": true, "stream_options": map[string]any{"include_usage": true},
	})
	t := int(c.next.Add(1))
	for {
		target := c.Targets[t%len(c.Targets)]
		rec.Target = target
		res := c.attempt(ctx, target, body, start, rec)
		switch {
		case res.err == nil:
			rec.Status = "ok"
			rec.E2EMs = ms(time.Since(start))
			return
		case res.transport && rec.ConnRetries < c.MaxConn && ctx.Err() == nil:
			rec.ConnRetries++
			t++ // try the next router
			resetStream(rec)
			continue
		case res.status == http.StatusTooManyRequests && rec.Retries429 < c.Max429 && ctx.Err() == nil:
			rec.Retries429++
			select {
			case <-time.After(res.retryAt):
			case <-ctx.Done():
			}
			continue
		}
		rec.HTTPStatus = res.status
		rec.Error = res.err.Error()
		switch {
		case res.status == http.StatusTooManyRequests:
			rec.Status = "http_429"
		case res.status >= 500:
			rec.Status = "http_5xx"
		case res.status >= 400:
			rec.Status = "http_4xx"
		default:
			rec.Status = "error"
		}
		rec.E2EMs = ms(time.Since(start))
		return
	}
}

func resetStream(rec *Record) {
	rec.TTFTMs, rec.ITLMs, rec.Tokens, rec.Text = -1, nil, 0, ""
}

func (c *Client) attempt(ctx context.Context, target string, body []byte, start time.Time, rec *Record) attemptResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(target, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return attemptResult{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return attemptResult{transport: true, err: err}
	}
	defer resp.Body.Close()
	rec.HTTPStatus = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		ra := time.Second
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			ra = time.Duration(s) * time.Second
		}
		return attemptResult{status: resp.StatusCode, retryAt: ra, err: fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))}
	}

	rd := bufio.NewReaderSize(resp.Body, 64<<10)
	var last time.Time
	var text strings.Builder
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			// Stream ended without [DONE]: the router died mid-stream.
			return attemptResult{transport: true, err: fmt.Errorf("stream broken: %w", err)}
		}
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			rec.Text = text.String()
			return attemptResult{status: 200}
		}
		var ev struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens int `json:"prompt_tokens"`
				Details      *struct {
					CachedTokens int `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
			} `json:"usage"`
			Relay *struct {
				Worker      string    `json:"worker"`
				Failovers   int       `json:"failovers"`
				PauseMs     []float64 `json:"failover_pause_ms"`
				MatchBlocks int       `json:"match_blocks"`
			} `json:"relay"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal([]byte(data), &ev) != nil {
			continue
		}
		if ev.Relay != nil {
			rec.Worker, rec.Failovers, rec.PauseMs, rec.MatchBlocks = ev.Relay.Worker, ev.Relay.Failovers, ev.Relay.PauseMs, ev.Relay.MatchBlocks
		}
		if ev.Error != nil {
			return attemptResult{status: 200, err: errors.New(ev.Error.Message)}
		}
		if ev.Usage != nil {
			rec.PromptToks = ev.Usage.PromptTokens
			if ev.Usage.Details != nil {
				rec.CachedToks = ev.Usage.Details.CachedTokens
			}
		}
		if len(ev.Choices) > 0 && ev.Choices[0].Delta.Content != "" {
			now := time.Now()
			if rec.TTFTMs < 0 {
				rec.TTFTMs = ms(now.Sub(start))
			} else {
				rec.ITLMs = append(rec.ITLMs, round1(ms(now.Sub(last))))
			}
			last = now
			rec.Tokens++
			text.WriteString(ev.Choices[0].Delta.Content)
		}
	}
}

func ms(d time.Duration) float64 { return round1(float64(d.Microseconds()) / 1000) }
func round1(f float64) float64   { return float64(int64(f*10+0.5)) / 10 }
