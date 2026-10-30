// Package tokenize turns prompt segments into token IDs for prefix hashing.
//
// Two implementations:
//   - Remote calls a worker's /tokenize endpoint (the model's real
//     tokenizer), caching per segment so a shared system prompt or earlier
//     conversation turn is tokenized once, not on every request.
//   - Approx packs every 4 bytes into one pseudo-token. It needs no network
//     call and preserves the only property routing needs: prompts sharing a
//     byte prefix share leading block hashes.
package tokenize

import (
	"container/list"
	"context"
	"hash/fnv"
	"sync"
)

// Tokenizer turns prompt segments into one token sequence.
type Tokenizer interface {
	Tokenize(ctx context.Context, segments []string) ([]int32, error)
}

// Approx is the network-free tokenizer: 4 bytes per token.
type Approx struct{}

func (Approx) Tokenize(_ context.Context, segments []string) ([]int32, error) {
	n := 0
	for _, s := range segments {
		n += len(s)
	}
	b := make([]byte, 0, n)
	for _, s := range segments {
		b = append(b, s...)
	}
	return Bytes(b), nil
}

// Bytes packs b into int32 pseudo-tokens, 4 bytes each.
func Bytes(b []byte) []int32 {
	out := make([]int32, 0, (len(b)+3)/4)
	for i := 0; i < len(b); i += 4 {
		var v uint32
		for j := 0; j < 4 && i+j < len(b); j++ {
			v |= uint32(b[i+j]) << (8 * j)
		}
		out = append(out, int32(v))
	}
	return out
}

// RemoteFunc tokenizes one string using the model's tokenizer.
type RemoteFunc func(ctx context.Context, text string) ([]int32, error)

// Remote tokenizes segments via a worker, with an LRU cache per segment.
// If the worker call fails it falls back to Approx for the whole request so
// that routing still works (with temporarily mismatched hashes).
type Remote struct {
	fn    RemoteFunc
	cache *lru
}

func NewRemote(fn RemoteFunc, cacheEntries int) *Remote {
	return &Remote{fn: fn, cache: newLRU(cacheEntries)}
}

func (r *Remote) Tokenize(ctx context.Context, segments []string) ([]int32, error) {
	var out []int32
	for _, s := range segments {
		if s == "" {
			continue
		}
		key := keyOf(s)
		toks, ok := r.cache.get(key)
		if !ok {
			var err error
			toks, err = r.fn(ctx, s)
			if err != nil {
				return Approx{}.Tokenize(ctx, segments)
			}
			r.cache.put(key, toks)
		}
		out = append(out, toks...)
	}
	return out, nil
}

func keyOf(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64() ^ uint64(len(s))<<48
}

// lru is a minimal thread-safe LRU map.
type lru struct {
	mu    sync.Mutex
	cap   int
	order *list.List
	items map[uint64]*list.Element
}

type entry struct {
	key  uint64
	toks []int32
}

func newLRU(capacity int) *lru {
	return &lru{cap: max(1, capacity), order: list.New(), items: map[uint64]*list.Element{}}
}

func (l *lru) get(k uint64) ([]int32, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[k]; ok {
		l.order.MoveToFront(el)
		return el.Value.(*entry).toks, true
	}
	return nil, false
}

func (l *lru) put(k uint64, toks []int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[k]; ok {
		el.Value.(*entry).toks = toks
		l.order.MoveToFront(el)
		return
	}
	l.items[k] = l.order.PushFront(&entry{k, toks})
	for len(l.items) > l.cap {
		el := l.order.Back()
		l.order.Remove(el)
		delete(l.items, el.Value.(*entry).key)
	}
}
