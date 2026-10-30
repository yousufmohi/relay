package prefix

import "testing"

func seq(n int, start int32) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = start + int32(i)
	}
	return out
}

func TestBlockHashesIgnoresPartialBlock(t *testing.T) {
	if got := len(BlockHashes(seq(10, 0), 4)); got != 2 {
		t.Fatalf("10 tokens / block 4: want 2 full blocks, got %d", got)
	}
	if got := len(BlockHashes(seq(3, 0), 4)); got != 0 {
		t.Fatalf("3 tokens / block 4: want 0 blocks, got %d", got)
	}
}

func TestBlockHashesAreChained(t *testing.T) {
	a := seq(12, 0)
	b := append([]int32{}, a...)
	b[1] = 999 // differ only in block 0

	ha, hb := BlockHashes(a, 4), BlockHashes(b, 4)
	// Blocks 1 and 2 have identical tokens, but because the hash is chained a
	// difference in block 0 must change every later hash.
	for i := range ha {
		if ha[i] == hb[i] {
			t.Fatalf("block %d hash equal despite different earlier block", i)
		}
	}
}

func TestBlockHashesSharedPrefix(t *testing.T) {
	a := seq(12, 0)
	b := append(seq(8, 0), 100, 101, 102, 103) // shares first 2 blocks
	ha, hb := BlockHashes(a, 4), BlockHashes(b, 4)
	if ha[0] != hb[0] || ha[1] != hb[1] {
		t.Fatal("shared prefix blocks should hash equal")
	}
	if ha[2] == hb[2] {
		t.Fatal("divergent block should hash differently")
	}
}

func TestBlockHashesPositionMatters(t *testing.T) {
	// The same block content at a different position (different history) must
	// not collide: [X X] vs [X] alone.
	x := []int32{7, 7, 7, 7}
	h1 := BlockHashes(x, 4)
	h2 := BlockHashes(append(append([]int32{}, x...), x...), 4)
	if h1[0] != h2[0] {
		t.Fatal("first block should match")
	}
	if h2[1] == h2[0] {
		t.Fatal("repeated block content at a new position must hash differently")
	}
}

func TestBlockHashesDeterministic(t *testing.T) {
	a, b := BlockHashes(seq(64, 5), 16), BlockHashes(seq(64, 5), 16)
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("hashing must be deterministic")
		}
	}
}

func TestMatchLengthStopsAtFirstMiss(t *testing.T) {
	ix := NewIndex(100)
	hs := BlockHashes(seq(16, 0), 4) // 4 blocks
	ix.Insert(hs[:3])
	if got := ix.MatchLength(hs); got != 3 {
		t.Fatalf("want 3, got %d", got)
	}
	// Even if a later block is present, a gap ends the match.
	ix2 := NewIndex(100)
	ix2.Insert([]Hash{hs[0], hs[2], hs[3]})
	if got := ix2.MatchLength(hs); got != 1 {
		t.Fatalf("gap at block 1: want 1, got %d", got)
	}
	if got := NewIndex(10).MatchLength(hs); got != 0 {
		t.Fatalf("empty index: want 0, got %d", got)
	}
}

func TestLRUEviction(t *testing.T) {
	ix := NewIndex(4)
	a := BlockHashes(seq(16, 0), 4)   // 4 blocks
	b := BlockHashes(seq(8, 1000), 4) // 2 blocks
	ix.Insert(a)
	if ix.Len() != 4 {
		t.Fatalf("want 4, got %d", ix.Len())
	}
	ix.Insert(b) // evicts 2 least-recently-used blocks: the TAIL of a
	if ix.Len() != 4 {
		t.Fatalf("capacity exceeded: %d", ix.Len())
	}
	if got := ix.MatchLength(a); got != 2 {
		t.Fatalf("a should keep its head (2 blocks), got %d", got)
	}
	if got := ix.MatchLength(b); got != 2 {
		t.Fatalf("b fully cached, got %d", got)
	}
}

func TestLRUTouchProtects(t *testing.T) {
	ix := NewIndex(4)
	a := BlockHashes(seq(8, 0), 4)    // 2 blocks
	b := BlockHashes(seq(8, 500), 4)  // 2 blocks
	c := BlockHashes(seq(8, 1000), 4) // 2 blocks
	ix.Insert(a)
	ix.Insert(b)
	ix.Insert(a) // a is now most recent
	ix.Insert(c) // must evict b, not a
	if ix.MatchLength(a) != 2 || ix.MatchLength(c) != 2 || ix.MatchLength(b) != 0 {
		t.Fatalf("a=%d b=%d c=%d", ix.MatchLength(a), ix.MatchLength(b), ix.MatchLength(c))
	}
}

func TestResizeAndReset(t *testing.T) {
	ix := NewIndex(10)
	hs := BlockHashes(seq(40, 0), 4)
	ix.Insert(hs)
	ix.Resize(3)
	if ix.Len() != 3 || ix.MatchLength(hs) != 3 {
		t.Fatalf("after shrink: len=%d match=%d", ix.Len(), ix.MatchLength(hs))
	}
	ix.Reset()
	if ix.Len() != 0 || ix.MatchLength(hs) != 0 {
		t.Fatal("reset should clear")
	}
}

func TestIndexesPerWorker(t *testing.T) {
	s := NewIndexes(16)
	hs := BlockHashes(seq(8, 0), 4)
	s.For("w1").Insert(hs)
	if s.MatchLength("w1", hs) != 2 || s.MatchLength("w2", hs) != 0 {
		t.Fatal("indexes must be independent per worker")
	}
	s.Remove("w1")
	if s.MatchLength("w1", hs) != 0 {
		t.Fatal("removed worker should start empty")
	}
}
