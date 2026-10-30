package workload

import (
	"strings"
	"testing"
	"time"
)

func spec(kind string, p Params) Spec {
	return Spec{Name: kind, Kind: kind, Rate: 5, MaxTokens: 8, Params: p}
}

func TestDeterministicPerSeed(t *testing.T) {
	a, _ := NewGenerator(spec("shared-system-prompt", Params{Prefixes: 4, PrefixTokens: 50, ZipfS: 1.2, UserTokens: [2]int{5, 10}}), 7)
	b, _ := NewGenerator(spec("shared-system-prompt", Params{Prefixes: 4, PrefixTokens: 50, ZipfS: 1.2, UserTokens: [2]int{5, 10}}), 7)
	for i := 0; i < 20; i++ {
		x, y := a.Next(), b.Next()
		if x.System != y.System || x.UserTurns[0] != y.UserTurns[0] {
			t.Fatal("same seed must give the same requests")
		}
	}
}

func TestPrefixesIdenticalAcrossSeeds(t *testing.T) {
	// Different seeds (and policies) must see the same system prompts.
	a, _ := NewGenerator(spec("hot-prefix", Params{Prefixes: 3, PrefixTokens: 40, HotFraction: 1}), 1)
	b, _ := NewGenerator(spec("hot-prefix", Params{Prefixes: 3, PrefixTokens: 40, HotFraction: 1}), 2)
	if a.Next().System != b.Next().System {
		t.Fatal("prefix 0 must not depend on the seed")
	}
}

func TestHotPrefixFraction(t *testing.T) {
	g, _ := NewGenerator(spec("hot-prefix", Params{Prefixes: 8, PrefixTokens: 20, HotFraction: 0.9}), 3)
	hot := 0
	for i := 0; i < 2000; i++ {
		if g.Next().PrefixID == 0 {
			hot++
		}
	}
	if hot < 1700 || hot > 1900 {
		t.Fatalf("hot share %d/2000", hot)
	}
}

func TestUniqueHasNoSharedPrefix(t *testing.T) {
	g, _ := NewGenerator(spec("unique", Params{PromptTokens: [2]int{50, 60}}), 1)
	a, b := g.Next().UserTurns[0], g.Next().UserTurns[0]
	if a[:20] == b[:20] {
		t.Fatal("unique prompts should diverge immediately")
	}
}

func TestMultiTurn(t *testing.T) {
	g, _ := NewGenerator(spec("multi-turn-chat", Params{Prefixes: 2, PrefixTokens: 20, Turns: [2]int{3, 3}, ThinkTime: Dur{time.Second}}), 1)
	c := g.Next()
	if len(c.UserTurns) != 3 || c.Think != time.Second || g.AvgTurns() != 3 {
		t.Fatalf("%+v", c)
	}
}

func TestTextLengthAndSchedule(t *testing.T) {
	g, _ := NewGenerator(spec("unique", Params{}), 1)
	if n := len(Text(g.rnd, 100)); n < 400 || n > 430 {
		t.Fatalf("100 tokens ≈ 400 bytes, got %d", n)
	}
	s := Spec{Rate: 2, Schedule: [][2]float64{{0, 2}, {60, 10}, {180, 2}}}
	if s.RateAt(30*time.Second) != 2 || s.RateAt(61*time.Second) != 10 || s.RateAt(200*time.Second) != 2 {
		t.Fatal("schedule")
	}
	if !strings.Contains(strings.Join(vocab, " "), "cache") {
		t.Fatal("vocab")
	}
}
