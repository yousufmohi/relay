// Package worker talks to model servers: streaming completions, tokenization
// and health checks, for both llama.cpp and vLLM.
//
// The router always sends plain *completion* requests (never chat): it has
// already applied the chat template, and a completion of
// "prompt + generated_so_far" is exactly what mid-stream failover needs.
package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yousufmohi/relay/router/internal/failover"
)

// Flavor selects the model server's API dialect.
type Flavor string

const (
	LlamaCpp Flavor = "llamacpp" // POST /completion (native), /tokenize {content}
	VLLM     Flavor = "vllm"     // POST /v1/completions (OpenAI), /tokenize {prompt}
)

// CompletionRequest is the router's internal request to a worker.
type CompletionRequest struct {
	Prompt      string
	MaxTokens   int
	Temperature float64
	TopP        float64
	Seed        *int64
	Stop        []string
}

// StatusError is a non-2xx answer from a worker.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string { return fmt.Sprintf("worker: HTTP %d: %s", e.Code, e.Body) }

// Retryable says whether failing over could help: 4xx (bad request, prompt
// too long) will fail identically elsewhere; 5xx, 429, timeouts and broken
// connections may not.
func Retryable(err error) bool {
	var se *StatusError
	if errors.As(err, &se) {
		return se.Code >= 500 || se.Code == http.StatusTooManyRequests
	}
	return true
}

// Client is safe for concurrent use.
type Client struct {
	Flavor Flavor
	Model  string // sent to vLLM, which requires it
	HTTP   *http.Client
}

func NewClient(flavor Flavor, model string) *Client {
	return &Client{
		Flavor: flavor,
		Model:  model,
		// No overall timeout: streams can be long. Stalls are detected by the
		// failover loop instead.
		HTTP: &http.Client{Transport: &http.Transport{
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		}},
	}
}

// Complete starts a streaming completion.
func (c *Client) Complete(ctx context.Context, baseURL string, r CompletionRequest) (failover.Stream, error) {
	var path string
	var body map[string]any
	switch c.Flavor {
	case VLLM:
		path = "/v1/completions"
		body = map[string]any{
			"model": c.Model, "prompt": r.Prompt, "max_tokens": r.MaxTokens,
			"temperature": r.Temperature, "stream": true,
			"stream_options": map[string]any{"include_usage": true},
		}
		if r.TopP > 0 {
			body["top_p"] = r.TopP
		}
	default:
		path = "/completion"
		body = map[string]any{
			"prompt": r.Prompt, "n_predict": r.MaxTokens, "temperature": r.Temperature,
			"stream": true, "cache_prompt": true,
		}
		if r.TopP > 0 {
			body["top_p"] = r.TopP
		}
	}
	if len(r.Stop) > 0 {
		body["stop"] = r.Stop
	}
	if r.Seed != nil {
		body["seed"] = *r.Seed
	}
	resp, err := c.post(ctx, baseURL+path, body)
	if err != nil {
		return nil, err
	}
	return &sseStream{flavor: c.Flavor, body: resp.Body, rd: bufio.NewReaderSize(resp.Body, 64<<10)}, nil
}

// Tokenize returns the model's token IDs for text (special tokens parsed, no
// BOS added).
func (c *Client) Tokenize(ctx context.Context, baseURL, text string) ([]int32, error) {
	var body map[string]any
	if c.Flavor == VLLM {
		body = map[string]any{"model": c.Model, "prompt": text, "add_special_tokens": false}
	} else {
		body = map[string]any{"content": text, "add_special": false}
	}
	resp, err := c.post(ctx, baseURL+"/tokenize", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Tokens []int32 `json:"tokens"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("worker: decode tokenize: %w", err)
	}
	return out.Tokens, nil
}

// Health checks GET /health (both servers return 200 when ready).
func (c *Client) Health(ctx context.Context, baseURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return &StatusError{Code: resp.StatusCode}
	}
	return nil
}

func (c *Client) post(ctx context.Context, url string, body any) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		return nil, &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(msg))}
	}
	return resp, nil
}

// sseStream parses a server-sent-events completion stream into Chunks.
type sseStream struct {
	flavor   Flavor
	body     io.ReadCloser
	rd       *bufio.Reader
	finished bool // saw the final content chunk
	done     bool
}

func (s *sseStream) Close() error { return s.body.Close() }

func (s *sseStream) Recv() (failover.Chunk, error) {
	for {
		if s.done {
			return failover.Chunk{}, io.EOF
		}
		data, err := s.nextEvent()
		if err != nil {
			if err == io.EOF {
				s.done = true
				if s.finished {
					return failover.Chunk{}, io.EOF
				}
				// The connection ended before the server said it was done:
				// the worker died mid-stream.
				return failover.Chunk{}, io.ErrUnexpectedEOF
			}
			return failover.Chunk{}, err
		}
		if data == "[DONE]" {
			s.done = true
			if !s.finished {
				return failover.Chunk{}, io.ErrUnexpectedEOF
			}
			return failover.Chunk{}, io.EOF
		}
		var c failover.Chunk
		var ok bool
		if s.flavor == VLLM {
			c, ok, err = parseVLLM(data)
		} else {
			c, ok, err = parseLlamaCpp(data)
		}
		if err != nil {
			return failover.Chunk{}, err
		}
		if c.FinishReason != "" {
			s.finished = true
			if s.flavor == LlamaCpp {
				s.done = true // llama.cpp's final chunk is the last event
			}
		}
		if ok {
			return c, nil
		}
	}
}

// nextEvent returns the data payload of the next SSE event.
func (s *sseStream) nextEvent() (string, error) {
	var data []string
	for {
		line, err := s.rd.ReadString('\n')
		if err != nil {
			if err == io.EOF && len(data) > 0 {
				return strings.Join(data, "\n"), nil
			}
			return "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if len(data) > 0 {
				return strings.Join(data, "\n"), nil
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		} else if strings.HasPrefix(line, "error:") {
			return "", fmt.Errorf("worker: stream error: %s", strings.TrimSpace(strings.TrimPrefix(line, "error:")))
		}
	}
}

// parseLlamaCpp handles llama.cpp's native /completion stream:
//
//	{"content":"Hi","stop":false}
//	{"content":"","stop":true,"stop_type":"limit","timings":{"cache_n":12,"prompt_n":30,...}}
func parseLlamaCpp(data string) (failover.Chunk, bool, error) {
	var m struct {
		Content      string `json:"content"`
		Stop         bool   `json:"stop"`
		StopType     string `json:"stop_type"`
		StoppedLimit bool   `json:"stopped_limit"`
		TokensCached int    `json:"tokens_cached"`
		TokensEval   int    `json:"tokens_evaluated"`
		Error        *struct {
			Message string `json:"message"`
		} `json:"error"`
		Timings *struct {
			CacheN  *int `json:"cache_n"`
			PromptN int  `json:"prompt_n"`
		} `json:"timings"`
	}
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		return failover.Chunk{}, false, fmt.Errorf("worker: bad llama.cpp chunk: %w", err)
	}
	if m.Error != nil {
		return failover.Chunk{}, false, fmt.Errorf("worker: %s", m.Error.Message)
	}
	c := failover.Chunk{Text: m.Content}
	if m.Content != "" {
		c.Tokens = 1
	}
	if m.Stop {
		c.FinishReason = "stop"
		if m.StopType == "limit" || m.StoppedLimit {
			c.FinishReason = "length"
		}
		if m.Timings != nil {
			u := &failover.Usage{PromptTokens: m.Timings.PromptN}
			if m.Timings.CacheN != nil {
				// prompt_n counts only the tokens actually evaluated.
				u.CachedTokens = *m.Timings.CacheN
				u.PromptTokens += *m.Timings.CacheN
			}
			if m.TokensEval > u.PromptTokens {
				u.PromptTokens = m.TokensEval
			}
			c.Usage = u
		}
	}
	return c, c.Text != "" || c.FinishReason != "" || c.Usage != nil, nil
}

// parseVLLM handles the OpenAI completions stream with include_usage:
//
//	{"choices":[{"text":"Hi","finish_reason":null}]}
//	{"choices":[{"text":"","finish_reason":"length"}]}
//	{"choices":[],"usage":{"prompt_tokens":30,"prompt_tokens_details":{"cached_tokens":16}}}
func parseVLLM(data string) (failover.Chunk, bool, error) {
	var m struct {
		Choices []struct {
			Text         string  `json:"text"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens int `json:"prompt_tokens"`
			Details      *struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		return failover.Chunk{}, false, fmt.Errorf("worker: bad vLLM chunk: %w", err)
	}
	if m.Error != nil {
		return failover.Chunk{}, false, fmt.Errorf("worker: %s", m.Error.Message)
	}
	var c failover.Chunk
	if len(m.Choices) > 0 {
		c.Text = m.Choices[0].Text
		if c.Text != "" {
			c.Tokens = 1
		}
		if fr := m.Choices[0].FinishReason; fr != nil && *fr != "" {
			c.FinishReason = *fr
			if c.FinishReason != "length" {
				c.FinishReason = "stop"
			}
		}
	}
	if m.Usage != nil {
		c.Usage = &failover.Usage{PromptTokens: m.Usage.PromptTokens}
		if m.Usage.Details != nil {
			c.Usage.CachedTokens = m.Usage.Details.CachedTokens
		}
	}
	return c, c.Text != "" || c.FinishReason != "" || c.Usage != nil, nil
}
