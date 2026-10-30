// Package workload defines the synthetic workloads and generates their
// requests deterministically from a seed.
package workload

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Spec is a workload definition file (experiments/workloads/*.yaml).
type Spec struct {
	Name        string  `yaml:"name"`
	Kind        string  `yaml:"kind"`
	Rate        float64 `yaml:"rate"` // requests per second
	Duration    Dur     `yaml:"duration"`
	MaxTokens   int     `yaml:"max_tokens"`
	Temperature float64 `yaml:"temperature"`
	// Schedule optionally varies the rate over time: [[t_seconds, rate], ...].
	Schedule [][2]float64 `yaml:"schedule"`
	Params   Params       `yaml:"params"`
}

// Params holds every generator knob; each kind uses a subset.
type Params struct {
	Prefixes     int     `yaml:"prefixes"`      // distinct shared prefixes (system prompts / docs)
	PrefixTokens int     `yaml:"prefix_tokens"` // length of each shared prefix
	ZipfS        float64 `yaml:"zipf_s"`        // popularity skew of prefixes (>1)
	HotFraction  float64 `yaml:"hot_fraction"`  // hot-prefix: share of traffic on prefix 0
	UserTokens   [2]int  `yaml:"user_tokens"`   // [min,max] user message length
	Turns        [2]int  `yaml:"turns"`         // multi-turn: [min,max] turns per conversation
	ThinkTime    Dur     `yaml:"think_time"`    // multi-turn: pause between turns
	PromptTokens [2]int  `yaml:"prompt_tokens"` // unique: [min,max] prompt length
}

// Dur is a time.Duration that unmarshals from "30s".
type Dur struct{ time.Duration }

func (d *Dur) UnmarshalYAML(n *yaml.Node) error {
	v, err := time.ParseDuration(n.Value)
	d.Duration = v
	return err
}

// Load reads a spec file.
func Load(path string) (Spec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Spec{}, err
	}
	var s Spec
	if err := yaml.Unmarshal(b, &s); err != nil {
		return Spec{}, fmt.Errorf("workload %s: %w", path, err)
	}
	if s.MaxTokens == 0 {
		s.MaxTokens = 64
	}
	if s.Params.UserTokens == [2]int{} {
		s.Params.UserTokens = [2]int{30, 120}
	}
	if s.Params.ZipfS <= 1 {
		s.Params.ZipfS = 1.1
	}
	return s, nil
}

// RateAt returns the request rate at elapsed time t.
func (s Spec) RateAt(t time.Duration) float64 {
	r := s.Rate
	for _, step := range s.Schedule {
		if t.Seconds() >= step[0] {
			r = step[1]
		}
	}
	return r
}

// Message is one chat message.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Conversation is a unit of work: one request, or several sequential turns
// (multi-turn chat) where each turn appends the model's previous answer.
type Conversation struct {
	ID        int
	PrefixID  int      // which shared prefix it uses (-1 = none)
	System    string   // system prompt ("" = none)
	UserTurns []string // one user message per turn
	Think     time.Duration
}

// Generator produces conversations deterministically.
type Generator struct {
	spec     Spec
	rnd      *rand.Rand
	prefixes []string
	zipf     *rand.Zipf
	next     int
}

func NewGenerator(spec Spec, seed int64) (*Generator, error) {
	g := &Generator{spec: spec, rnd: rand.New(rand.NewSource(seed))}
	p := spec.Params
	switch spec.Kind {
	case "shared-system-prompt", "hot-prefix", "rag-shared-docs", "multi-turn-chat":
		n := max(1, p.Prefixes)
		// Prefixes are generated from a FIXED seed so every run (and every
		// policy) sees the same system prompts / documents.
		pr := rand.New(rand.NewSource(1000 + int64(len(spec.Kind))))
		for i := 0; i < n; i++ {
			body := Text(pr, max(16, p.PrefixTokens))
			if spec.Kind == "rag-shared-docs" {
				body = fmt.Sprintf("Document %d.\n%s", i, body)
			} else {
				body = fmt.Sprintf("You are assistant persona #%d. Follow these instructions carefully.\n%s", i, body)
			}
			g.prefixes = append(g.prefixes, body)
		}
		if n > 1 {
			zs := p.ZipfS
			if zs <= 1 { // rand.NewZipf requires s > 1
				zs = 1.1
			}
			g.zipf = rand.NewZipf(g.rnd, zs, 1, uint64(n-1))
		}
	case "unique":
	default:
		return nil, fmt.Errorf("workload: unknown kind %q", spec.Kind)
	}
	return g, nil
}

func (g *Generator) pickPrefix() int {
	switch {
	case len(g.prefixes) <= 1:
		return 0
	case g.spec.Kind == "hot-prefix":
		if g.rnd.Float64() < g.spec.Params.HotFraction {
			return 0
		}
		return 1 + g.rnd.Intn(len(g.prefixes)-1)
	default:
		return int(g.zipf.Uint64())
	}
}

func (g *Generator) between(r [2]int) int {
	if r[1] <= r[0] {
		return r[0]
	}
	return r[0] + g.rnd.Intn(r[1]-r[0]+1)
}

// AvgTurns is the expected number of requests per conversation.
func (g *Generator) AvgTurns() float64 {
	if g.spec.Kind != "multi-turn-chat" {
		return 1
	}
	t := g.spec.Params.Turns
	if t[1] <= t[0] {
		return math.Max(1, float64(t[0]))
	}
	return float64(t[0]+t[1]) / 2
}

// Next returns the next conversation.
func (g *Generator) Next() Conversation {
	g.next++
	p := g.spec.Params
	c := Conversation{ID: g.next, PrefixID: -1}
	switch g.spec.Kind {
	case "unique":
		c.UserTurns = []string{fmt.Sprintf("Request %d (%d). %s", g.next, g.rnd.Int63(), Text(g.rnd, g.between(p.PromptTokens)))}
	case "rag-shared-docs":
		c.PrefixID = g.pickPrefix()
		c.System = "Answer the question using only the provided document."
		q := Text(g.rnd, g.between(p.UserTokens))
		c.UserTurns = []string{"Context:\n" + g.prefixes[c.PrefixID] + "\n\nQuestion: " + q}
	case "multi-turn-chat":
		c.PrefixID = g.pickPrefix()
		c.System = g.prefixes[c.PrefixID]
		n := max(1, g.between(p.Turns))
		for i := 0; i < n; i++ {
			c.UserTurns = append(c.UserTurns, fmt.Sprintf("(conversation %d, turn %d) %s", g.next, i+1, Text(g.rnd, g.between(p.UserTokens))))
		}
		c.Think = p.ThinkTime.Duration
	default: // shared-system-prompt, hot-prefix
		c.PrefixID = g.pickPrefix()
		c.System = g.prefixes[c.PrefixID]
		c.UserTurns = []string{fmt.Sprintf("User %d asks: %s", g.next, Text(g.rnd, g.between(p.UserTokens)))}
	}
	return c
}

var vocab = strings.Fields(`time person year way day thing man world life hand part child eye woman place
work week case point government company number group problem fact be have do say get make go know take
see come think look want give use find tell ask work seem feel try leave call good new first last long
great little own other old right big high different small large next early young important few public
bad same able cache router worker token latency throughput memory request stream prefix model system
compute network cluster replica failover queue batch kernel tensor vector index storage database query
answer question document policy budget market customer product service report analysis design review`)

// Text returns roughly `tokens` tokens of pseudo-English (≈4 bytes/token).
func Text(r *rand.Rand, tokens int) string {
	target := tokens * 4
	var b strings.Builder
	b.Grow(target + 16)
	for b.Len() < target {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(vocab[r.Intn(len(vocab))])
	}
	return b.String()
}
