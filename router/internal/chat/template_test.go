package chat

import (
	"strings"
	"testing"
)

func TestChatMLWithDefaultSystem(t *testing.T) {
	tmpl, _ := Get("chatml")
	got := Join(tmpl.Segments([]Message{{Role: "user", Content: "hi"}}))
	want := "<|im_start|>system\nYou are Qwen, created by Alibaba Cloud. You are a helpful assistant.<|im_end|>\n" +
		"<|im_start|>user\nhi<|im_end|>\n<|im_start|>assistant\n"
	if got != want {
		t.Fatalf("got %q", got)
	}
}

func TestChatMLKeepsExplicitSystem(t *testing.T) {
	tmpl, _ := Get("chatml")
	got := Join(tmpl.Segments([]Message{{Role: "system", Content: "S"}, {Role: "user", Content: "U"}}))
	if strings.Contains(got, "Qwen") || !strings.HasPrefix(got, "<|im_start|>system\nS<|im_end|>\n") {
		t.Fatalf("got %q", got)
	}
}

func TestSegmentsSharePrefixAcrossTurns(t *testing.T) {
	// Turn 2's prompt must start with turn 1's prompt byte-for-byte, so the
	// worker's cached prefix from turn 1 is reusable.
	tmpl, _ := Get("chatml-plain")
	t1 := Join(tmpl.Segments([]Message{{Role: "user", Content: "q1"}}))
	t2 := Join(tmpl.Segments([]Message{{Role: "user", Content: "q1"}, {Role: "assistant", Content: "a1"}, {Role: "user", Content: "q2"}}))
	if !strings.HasPrefix(t2, t1) {
		t.Fatalf("turn 2 does not extend turn 1:\n%q\n%q", t1, t2)
	}
}

func TestLlama3(t *testing.T) {
	tmpl, _ := Get("llama3")
	got := Join(tmpl.Segments([]Message{{Role: "user", Content: "hi"}}))
	want := "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\nhi<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"
	if got != want || tmpl.Stop()[0] != "<|eot_id|>" {
		t.Fatalf("got %q", got)
	}
	if _, err := Get("nope"); err == nil {
		t.Fatal("unknown template")
	}
}
