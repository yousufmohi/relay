// Package prefix turns a tokenized prompt into chained block hashes and keeps,
// per worker, an LRU set of the block hashes the router believes are cached
// on that worker.
//
// The key idea: split the prompt into fixed-size token blocks and hash them
// as a chain,
//
//	h_0 = H(seed      || block_0)
//	h_i = H(h_{i-1}   || block_i)
//
// so h_i identifies the *entire prefix* up to and including block i. Two
// prompts share h_i exactly when their first (i+1)*blockSize tokens are
// identical. Looking up hashes in order and stopping at the first miss gives
// the length of the longest cached prefix, in blocks.
package prefix

import "encoding/binary"

// Hash identifies a prompt prefix that ends on a block boundary.
type Hash uint64

// FNV-1a 64-bit constants. FNV is used (instead of hash/maphash) because it
// is stable across processes: every router replica must compute the same
// hashes so consistent_hash routing agrees between replicas.
const (
	fnvOffset uint64 = 14695981039346656037
	fnvPrime  uint64 = 1099511628211
)

// fnvAdd mixes 8 bytes of v into h.
func fnvAdd(h uint64, v uint64) uint64 {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime
	}
	return h
}

// BlockHashes returns one chained hash per *full* block of tokens. A trailing
// partial block is ignored: workers cache in whole blocks, and a partial block
// is likely to differ in the next request anyway.
func BlockHashes(tokens []int32, blockSize int) []Hash {
	if blockSize <= 0 {
		panic("prefix: blockSize must be positive")
	}
	n := len(tokens) / blockSize
	out := make([]Hash, n)
	prev := fnvOffset
	for i := 0; i < n; i++ {
		h := fnvAdd(fnvOffset, prev) // chain: start from the previous block's hash
		for _, t := range tokens[i*blockSize : (i+1)*blockSize] {
			h = fnvAdd(h, uint64(uint32(t)))
		}
		out[i] = Hash(h)
		prev = h
	}
	return out
}

// KeyOf hashes an arbitrary token sequence (used as a routing key when a
// prompt is shorter than one block).
func KeyOf(tokens []int32) Hash {
	h := fnvOffset
	for _, t := range tokens {
		h = fnvAdd(h, uint64(uint32(t)))
	}
	return Hash(h)
}
