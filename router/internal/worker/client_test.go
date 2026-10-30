package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yousufmohi/relay/router/internal/failover"
)

func collect(t *testing.T, s failover.Stream) ([]failover.Chunk, error) {
	t.Helper()
	var out []failover.Chunk
	for {
		c, err := s.Recv()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, c)
	}
}

func TestLlamaCppStream(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/completion" {
			t.Errorf("path %s", r.URL.Path)
		}
		json.NewDecoder(r.Body).Decode(&got)
		io.WriteString(w, "data: {\"content\":\"Hel\",\"stop\":false}\n\n")
		io.WriteString(w, "data: {\"content\":\"lo\",\"stop\":false}\n\n")
		io.WriteString(w, "data: {\"content\":\"\",\"stop\":true,\"stop_type\":\"limit\",\"timings\":{\"cache_n\":64,\"prompt_n\":10}}\n\n")
	}))
	defer srv.Close()
	c := NewClient(LlamaCpp, "")
	s, err := c.Complete(context.Background(), srv.URL, CompletionRequest{Prompt: "p", MaxTokens: 2, Stop: []string{"<|im_end|>"}})
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := collect(t, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 || chunks[0].Text != "Hel" || chunks[2].FinishReason != "length" {
		t.Fatalf("%+v", chunks)
	}
	if u := chunks[2].Usage; u == nil || u.CachedTokens != 64 || u.PromptTokens != 74 {
		t.Fatalf("usage %+v", chunks[2].Usage)
	}
	if got["n_predict"].(float64) != 2 || got["cache_prompt"] != true || got["stream"] != true {
		t.Fatalf("request body %v", got)
	}
}

func TestLlamaCppTruncatedStreamIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: {\"content\":\"Hel\",\"stop\":false}\n\n")
		// connection closes without a stop:true chunk
	}))
	defer srv.Close()
	s, _ := NewClient(LlamaCpp, "").Complete(context.Background(), srv.URL, CompletionRequest{Prompt: "p"})
	if _, err := collect(t, s); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
}

func TestVLLMStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/completions" {
			t.Errorf("path %s", r.URL.Path)
		}
		for _, l := range []string{
			`{"choices":[{"text":"Hi","finish_reason":null}]}`,
			`{"choices":[{"text":"!","finish_reason":"stop"}]}`,
			`{"choices":[],"usage":{"prompt_tokens":30,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":16}}}`,
			`[DONE]`,
		} {
			io.WriteString(w, "data: "+l+"\n\n")
		}
	}))
	defer srv.Close()
	s, _ := NewClient(VLLM, "m").Complete(context.Background(), srv.URL, CompletionRequest{Prompt: "p", MaxTokens: 5})
	chunks, err := collect(t, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 || chunks[1].FinishReason != "stop" || chunks[2].Usage.CachedTokens != 16 {
		t.Fatalf("%+v", chunks)
	}
}

func TestVLLMDoneBeforeFinishIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: {\"choices\":[{\"text\":\"Hi\",\"finish_reason\":null}]}\n\ndata: [DONE]\n\n")
	}))
	defer srv.Close()
	s, _ := NewClient(VLLM, "m").Complete(context.Background(), srv.URL, CompletionRequest{Prompt: "p"})
	if _, err := collect(t, s); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v", err)
	}
}

func TestStatusErrorsAndRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "tokenize") {
			io.WriteString(w, `{"tokens":[1,2,3]}`)
			return
		}
		http.Error(w, "too long", http.StatusBadRequest)
	}))
	defer srv.Close()
	c := NewClient(LlamaCpp, "")
	_, err := c.Complete(context.Background(), srv.URL, CompletionRequest{Prompt: "p"})
	var se *StatusError
	if !errors.As(err, &se) || se.Code != 400 || Retryable(err) {
		t.Fatalf("got %v", err)
	}
	if !Retryable(&StatusError{Code: 503}) || !Retryable(io.ErrUnexpectedEOF) {
		t.Fatal("5xx and broken streams are retryable")
	}
	toks, err := c.Tokenize(context.Background(), srv.URL, "abc")
	if err != nil || len(toks) != 3 {
		t.Fatal(toks, err)
	}
}
