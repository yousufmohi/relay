package prefix

import (
	"container/list"
	"sync"
)

// Index is an LRU set of block hashes the router believes are cached on ONE
// worker. It is soft state: built from this router's own routing decisions,
// resized from the sidecar's reported cache capacity, and cleared when the
// worker restarts. Getting it wrong only costs cache hits, never correctness.
type Index struct {
	mu       sync.Mutex
	capacity int                    // max number of blocks remembered
	order    *list.List             // front = most recently used
	items    map[Hash]*list.Element // hash -> element holding that hash
}

// NewIndex returns an index that remembers at most capacity blocks.
func NewIndex(capacity int) *Index {
	if capacity < 1 {
		capacity = 1
	}
	return &Index{capacity: capacity, order: list.New(), items: make(map[Hash]*list.Element)}
}

// MatchLength returns how many LEADING hashes of hs are present. Because the
// hashes are chained, stopping at the first miss is exact: if block i is not
// cached, no later block of this prompt can be reused by the worker either.
// It does not change recency; Insert does that once a worker is chosen.
func (ix *Index) MatchLength(hs []Hash) int {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	n := 0
	for _, h := range hs {
		if _, ok := ix.items[h]; !ok {
			break
		}
		n++
	}
	return n
}

// Insert records that all of hs are now cached on the worker, marking them
// most recently used and evicting the least recently used blocks if needed.
//
// Blocks are touched from last to first, so the FIRST block of a prompt ends
// up most recent. Under eviction pressure a prompt therefore loses its tail
// before its head, matching how a real prefix cache behaves (a block is only
// useful if all blocks before it are cached too).
func (ix *Index) Insert(hs []Hash) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for i := len(hs) - 1; i >= 0; i-- {
		h := hs[i]
		if el, ok := ix.items[h]; ok {
			ix.order.MoveToFront(el)
			continue
		}
		ix.items[h] = ix.order.PushFront(h)
	}
	ix.evictLocked()
}

// Resize changes the capacity (e.g. when the sidecar reports the worker's
// real KV cache size), evicting LRU blocks if it shrank.
func (ix *Index) Resize(capacity int) {
	if capacity < 1 {
		capacity = 1
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.capacity = capacity
	ix.evictLocked()
}

// Reset forgets everything (worker restarted: its cache is empty).
func (ix *Index) Reset() {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.order.Init()
	ix.items = make(map[Hash]*list.Element)
}

// Len returns the number of blocks currently remembered.
func (ix *Index) Len() int {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return len(ix.items)
}

// Capacity returns the current capacity in blocks.
func (ix *Index) Capacity() int {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.capacity
}

func (ix *Index) evictLocked() {
	for len(ix.items) > ix.capacity {
		el := ix.order.Back()
		ix.order.Remove(el)
		delete(ix.items, el.Value.(Hash))
	}
}

// Indexes holds one Index per worker.
type Indexes struct {
	mu              sync.RWMutex
	byWorker        map[string]*Index
	defaultCapacity int
}

// NewIndexes creates per-worker indexes that start at defaultCapacity blocks
// until a sidecar reports the real size.
func NewIndexes(defaultCapacity int) *Indexes {
	return &Indexes{byWorker: make(map[string]*Index), defaultCapacity: defaultCapacity}
}

// For returns (creating if necessary) the index for worker id.
func (s *Indexes) For(id string) *Index {
	s.mu.RLock()
	ix, ok := s.byWorker[id]
	s.mu.RUnlock()
	if ok {
		return ix
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ix, ok = s.byWorker[id]; !ok {
		ix = NewIndex(s.defaultCapacity)
		s.byWorker[id] = ix
	}
	return ix
}

// MatchLength is a convenience for For(id).MatchLength(hs).
func (s *Indexes) MatchLength(id string, hs []Hash) int { return s.For(id).MatchLength(hs) }

// Remove drops the index for a worker that left the pool.
func (s *Indexes) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byWorker, id)
}
