package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// relayExt is Relay's extension to the usage object: which workers served
// the request and what failover did. The load generator records it.
type relayExt struct {
	Policy      string    `json:"policy"`
	Worker      string    `json:"worker"`
	Workers     []string  `json:"workers"`
	Failovers   int       `json:"failovers"`
	PauseMs     []float64 `json:"failover_pause_ms,omitempty"`
	MatchBlocks int       `json:"match_blocks"`
}

type usageDetails struct {
	CachedTokens int `json:"cached_tokens"`
}

type usageJSON struct {
	PromptTokens     int           `json:"prompt_tokens"`
	CompletionTokens int           `json:"completion_tokens"`
	TotalTokens      int           `json:"total_tokens"`
	Details          *usageDetails `json:"prompt_tokens_details,omitempty"`
}

// streamWriter writes either an SSE stream or (stream=false) one JSON body
// in OpenAI's format. Headers are sent lazily on the first token so that a
// request that fails before producing anything gets a proper HTTP error.
type streamWriter struct {
	w       http.ResponseWriter
	kind    string // "chat" | "text"
	stream  bool
	model   string
	id      string
	created int64
	worker  string

	started bool
	broken  bool
	buf     strings.Builder // non-streaming: accumulated text
}

func (sw *streamWriter) start() {
	if sw.started {
		return
	}
	sw.started = true
	h := sw.w.Header()
	h.Set("X-Relay-Worker", sw.worker)
	if sw.stream {
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no")
		sw.w.WriteHeader(http.StatusOK)
	}
}

// text forwards generated text; an error means the client disconnected.
func (sw *streamWriter) text(t string) error {
	if !sw.stream {
		sw.buf.WriteString(t)
		sw.start()
		return nil
	}
	first := !sw.started
	sw.start()
	return sw.event(sw.chunk(t, first, nil))
}

func (sw *streamWriter) chunk(text string, first bool, finish *string) map[string]any {
	choice := map[string]any{"index": 0, "finish_reason": finish}
	obj := "text_completion"
	if sw.kind == "chat" {
		obj = "chat.completion.chunk"
		delta := map[string]any{}
		if first {
			delta["role"] = "assistant"
		}
		if text != "" {
			delta["content"] = text
		}
		choice["delta"] = delta
	} else {
		choice["text"] = text
	}
	return map[string]any{"id": sw.id, "object": obj, "created": sw.created, "model": sw.model, "choices": []any{choice}}
}

func (sw *streamWriter) event(v any) error {
	if sw.broken {
		return io.ErrClosedPipe
	}
	b, _ := json.Marshal(v)
	if _, err := sw.w.Write(append(append([]byte("data: "), b...), '\n', '\n')); err != nil {
		sw.broken = true
		return err
	}
	if f, ok := sw.w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

func (sw *streamWriter) finish(reason string, usage usageJSON, ext relayExt, includeUsage bool) {
	if !sw.stream {
		sw.start()
		choice := map[string]any{"index": 0, "finish_reason": reason}
		obj := "text_completion"
		if sw.kind == "chat" {
			obj = "chat.completion"
			choice["message"] = map[string]any{"role": "assistant", "content": sw.buf.String()}
		} else {
			choice["text"] = sw.buf.String()
		}
		writeJSON(sw.w, http.StatusOK, map[string]any{"id": sw.id, "object": obj, "created": sw.created,
			"model": sw.model, "choices": []any{choice}, "usage": usage, "relay": ext})
		return
	}
	first := !sw.started
	sw.start()
	sw.event(sw.chunk("", first, &reason))
	if includeUsage {
		sw.event(map[string]any{"id": sw.id, "object": "chat.completion.chunk", "created": sw.created,
			"model": sw.model, "choices": []any{}, "usage": usage, "relay": ext})
	}
	sw.w.Write([]byte("data: [DONE]\n\n"))
	if f, ok := sw.w.(http.Flusher); ok {
		f.Flush()
	}
}

// fail ends the response with an error: an HTTP error if nothing was sent
// yet, otherwise an SSE error event (the stream already returned 200).
func (sw *streamWriter) fail(code int, msg string, ext relayExt) {
	if !sw.started {
		writeJSON(sw.w, code, map[string]any{"error": map[string]any{"message": msg, "type": "upstream_error"}, "relay": ext})
		return
	}
	if !sw.stream {
		// Partial text was buffered but cannot be returned as a success.
		writeJSON(sw.w, code, map[string]any{"error": map[string]any{"message": msg, "type": "upstream_error"}, "relay": ext})
		return
	}
	sw.event(map[string]any{"error": map[string]any{"message": msg, "type": "relay_failover_exhausted"}, "relay": ext})
	sw.w.Write([]byte("data: [DONE]\n\n"))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, typ, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]any{"message": msg, "type": typ}})
}

func newID(kind string) string {
	var b [12]byte
	rand.Read(b[:])
	p := "cmpl-"
	if kind == "chat" {
		p = "chatcmpl-"
	}
	return p + hex.EncodeToString(b[:])
}
