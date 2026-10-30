// Package chat renders OpenAI-style message lists into a single prompt string
// using the model's chat template.
//
// The prompt is returned as SEGMENTS (role header, content, footer, ...)
// whose concatenation is the prompt. Segments are the unit of tokenization
// caching: the same system prompt or earlier conversation turn produces
// byte-identical segments across requests, so each is tokenized once. Special
// tokens at segment boundaries mean tokenizing segments separately gives the
// same tokens as tokenizing the whole prompt.
package chat

import "fmt"

// Message is one chat message.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Template renders messages for one model family.
type Template interface {
	// Segments returns the prompt pieces, ending with the assistant header
	// (the "generation prompt").
	Segments(msgs []Message) []string
	// Stop returns end-of-turn strings to pass as stop sequences.
	Stop() []string
}

// Get returns a template by name.
func Get(name string) (Template, error) {
	switch name {
	case "chatml", "qwen", "":
		return chatML{defaultSystem: "You are Qwen, created by Alibaba Cloud. You are a helpful assistant."}, nil
	case "chatml-plain":
		return chatML{}, nil
	case "llama3":
		return llama3{}, nil
	default:
		return nil, fmt.Errorf("chat: unknown template %q", name)
	}
}

// Join concatenates segments into the prompt string.
func Join(segs []string) string {
	n := 0
	for _, s := range segs {
		n += len(s)
	}
	b := make([]byte, 0, n)
	for _, s := range segs {
		b = append(b, s...)
	}
	return string(b)
}

// chatML is used by Qwen2/2.5:
//
//	<|im_start|>system\n...<|im_end|>\n<|im_start|>user\n...<|im_end|>\n<|im_start|>assistant\n
type chatML struct{ defaultSystem string }

func (t chatML) Segments(msgs []Message) []string {
	var out []string
	if t.defaultSystem != "" && (len(msgs) == 0 || msgs[0].Role != "system") {
		out = append(out, "<|im_start|>system\n", t.defaultSystem, "<|im_end|>\n")
	}
	for _, m := range msgs {
		out = append(out, "<|im_start|>"+m.Role+"\n", m.Content, "<|im_end|>\n")
	}
	return append(out, "<|im_start|>assistant\n")
}

func (chatML) Stop() []string { return []string{"<|im_end|>", "<|endoftext|>"} }

// llama3 is used by Llama 3.x instruct models.
type llama3 struct{}

func (llama3) Segments(msgs []Message) []string {
	out := []string{"<|begin_of_text|>"}
	for _, m := range msgs {
		out = append(out, "<|start_header_id|>"+m.Role+"<|end_header_id|>\n\n", m.Content, "<|eot_id|>")
	}
	return append(out, "<|start_header_id|>assistant<|end_header_id|>\n\n")
}

func (llama3) Stop() []string { return []string{"<|eot_id|>", "<|end_of_text|>"} }
