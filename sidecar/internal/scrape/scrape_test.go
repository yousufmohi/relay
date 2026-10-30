package scrape

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	m, err := Parse(strings.NewReader(`# HELP x
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="qwen",engine="0"} 3
vllm:num_requests_running{model_name="qwen",engine="1"} 2
vllm:cache_config_info{block_size="16",num_gpu_blocks="1000"} 1.0
llamacpp:kv_cache_usage_ratio 0.25
bad line
`))
	if err != nil {
		t.Fatal(err)
	}
	if m.Values["vllm:num_requests_running"] != 5 {
		t.Fatalf("series must be summed: %v", m.Values)
	}
	if m.Values["llamacpp:kv_cache_usage_ratio"] != 0.25 {
		t.Fatal("colon names")
	}
	if m.Labels["vllm:cache_config_info"]["num_gpu_blocks"] != "1000" {
		t.Fatalf("labels %v", m.Labels)
	}
}

func TestLlamaCppScrape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			io.WriteString(w, "llamacpp:requests_processing 2\nllamacpp:requests_deferred 5\nllamacpp:kv_cache_usage_ratio 0.5\nllamacpp:prompt_tokens_total 100\nllamacpp:tokens_predicted_total 20\n")
		case "/props":
			io.WriteString(w, `{"total_slots":4,"model_path":"qwen.gguf","default_generation_settings":{"n_ctx":2048}}`)
		}
	}))
	defer srv.Close()
	s, err := New("llamacpp", srv.URL).Scrape(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s.Running != 2 || s.Waiting != 5 || s.KVUsage != 0.5 || s.CapacityTokens != 8192 || s.Progress != 120 || s.Model != "qwen.gguf" {
		t.Fatalf("%+v", s)
	}
}

func TestVLLMScrape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `vllm:num_requests_running{model_name="q"} 4
vllm:num_requests_waiting{model_name="q"} 1
vllm:kv_cache_usage_perc{model_name="q"} 0.7
vllm:prefix_cache_hits_total{model_name="q"} 300
vllm:prefix_cache_queries_total{model_name="q"} 1000
vllm:cache_config_info{block_size="16",num_gpu_blocks="2000"} 1
`)
	}))
	defer srv.Close()
	s, err := New("vllm", srv.URL).Scrape(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s.Running != 4 || s.Waiting != 1 || s.KVUsage != 0.7 || s.HitTokens != 300 || s.QueryTokens != 1000 || s.CapacityTokens != 32000 || s.Model != "q" {
		t.Fatalf("%+v", s)
	}
}
