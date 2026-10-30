package tokenize

import (
	"context"
	"errors"
	"testing"
)

func TestApproxSharedPrefix(t *testing.T) {
	a, _ := Approx{}.Tokenize(context.Background(), []string{"hello world, ", "how are you"})
	b, _ := Approx{}.Tokenize(context.Background(), []string{"hello world, how are", " they"})
	// The first 16 bytes ("hello world, how") are identical → first 4 tokens equal.
	for i := 0; i < 4; i++ {
		if a[i] != b[i] {
			t.Fatalf("token %d differs", i)
		}
	}
	if len(Bytes([]byte("abcde"))) != 2 {
		t.Fatal("5 bytes = 2 tokens")
	}
}

func TestRemoteCachesSegments(t *testing.T) {
	calls := 0
	r := NewRemote(func(_ context.Context, s string) ([]int32, error) {
		calls++
		return []int32{int32(len(s))}, nil
	}, 10)
	r.Tokenize(context.Background(), []string{"sys", "q1"})
	toks, _ := r.Tokenize(context.Background(), []string{"sys", "q2"})
	if calls != 3 {
		t.Fatalf("shared segment should be tokenized once; calls=%d", calls)
	}
	if len(toks) != 2 || toks[0] != 3 {
		t.Fatalf("%v", toks)
	}
}

func TestRemoteFallsBackToApprox(t *testing.T) {
	r := NewRemote(func(context.Context, string) ([]int32, error) { return nil, errors.New("down") }, 10)
	toks, err := r.Tokenize(context.Background(), []string{"abcdefgh"})
	if err != nil || len(toks) != 2 {
		t.Fatal(toks, err)
	}
}

func TestLRUEvicts(t *testing.T) {
	l := newLRU(2)
	l.put(1, nil)
	l.put(2, nil)
	l.get(1)
	l.put(3, nil)
	if _, ok := l.get(2); ok {
		t.Fatal("2 should be evicted")
	}
	if _, ok := l.get(1); !ok {
		t.Fatal("1 was touched, must stay")
	}
}
