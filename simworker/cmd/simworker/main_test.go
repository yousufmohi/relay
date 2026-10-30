package main

import (
	"strings"
	"testing"
)

func TestBlockCachePrefixMatch(t *testing.T) {
	c := newBlockCache(100)
	a := blockHashes(toTokens(strings.Repeat("abcd", 64))) // 64 tokens = 4 blocks
	if got := c.matchAndInsert(a); got != 0 {
		t.Fatalf("cold cache: %d", got)
	}
	if got := c.matchAndInsert(a); got != 4 {
		t.Fatalf("warm cache: %d", got)
	}
	b := blockHashes(toTokens(strings.Repeat("abcd", 32) + strings.Repeat("wxyz", 32)))
	if got := c.matchAndInsert(b); got != 2 {
		t.Fatalf("shared first half: %d", got)
	}
}

func TestBlockCacheEvicts(t *testing.T) {
	c := newBlockCache(4)
	a := blockHashes(toTokens(strings.Repeat("aaaa", 64)))
	c.matchAndInsert(a)
	c.matchAndInsert(blockHashes(toTokens(strings.Repeat("bbbb", 64))))
	if got := c.matchAndInsert(a); got != 0 {
		t.Fatalf("a should have been evicted, matched %d", got)
	}
}

func TestDeterministicOutput(t *testing.T) {
	first := nextWord("hello world")
	if first == "" || nextWord("hello world") != first {
		t.Fatal("greedy stand-in must be deterministic")
	}
}
