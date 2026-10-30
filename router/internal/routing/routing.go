// Package routing decides which worker serves a request.
//
// Every policy implements the same tiny interface: given the request's
// prefix block hashes and a snapshot of the eligible workers, pick one. The
// router builds that snapshot (health filtering, load numbers, prefix-match
// lengths) so policies are pure decision functions that are easy to test.
package routing

import (
	"errors"
	"fmt"

	"github.com/yousufmohi/relay/router/internal/prefix"
)

// ErrNoWorkers is returned when no eligible worker exists.
var ErrNoWorkers = errors.New("routing: no eligible workers")

// Worker is a policy's view of one eligible worker at decision time.
type Worker struct {
	ID string
	// Load is the number of outstanding requests at the worker (running +
	// queued). The router uses max(its own in-flight count, the sidecar's
	// running+waiting) so it is correct both for one router (own count is
	// exact and instant) and for several (sidecar sees every router's work).
	Load int
	// InFlight is this router's own in-flight requests to the worker; used to
	// break ties.
	InFlight int
	// KVUsage is the worker's KV-cache utilisation, 0..1 (from the sidecar).
	KVUsage float64
	// MatchBlocks is the number of leading prompt blocks this router believes
	// are already cached on the worker. Only the relay policy uses it.
	MatchBlocks int
}

// Request is what a policy needs to know about the request.
type Request struct {
	// Chained block hashes of the prompt (see package prefix).
	Blocks []prefix.Hash
	// Hash of the whole prompt; consistent_hash uses it when the prompt is
	// shorter than one block.
	Key prefix.Hash
}

// Decision is the outcome plus enough detail to trace and debug it.
type Decision struct {
	WorkerID    string
	MatchBlocks int
	// Scores per candidate worker (relay policy only); recorded in traces.
	Scores map[string]float64
	// Excluded lists workers removed by the bounded-load rule.
	Excluded []string
	Reason   string
}

// Policy picks a worker. Implementations must be safe for concurrent use.
type Policy interface {
	Name() string
	// NeedsMatch reports whether the router must fill Worker.MatchBlocks
	// (it costs one index lookup per worker, so baselines skip it).
	NeedsMatch() bool
	Pick(req Request, workers []Worker) (Decision, error)
}

// Config holds every tunable used by the policies.
type Config struct {
	Alpha, Beta, Gamma float64 // relay score weights
	LoadBoundC         float64 // relay bounded-load factor c
	LoadBoundFloor     int     // relay: the bound never excludes a worker whose load would stay ≤ this
	HashBlocks         int     // consistent_hash: how many leading blocks form the key
	VirtualNodes       int     // consistent_hash: ring points per worker
	Seed               int64   // random tie-breaking seed (0 = time-based)
}

// DefaultConfig matches the defaults in docs/configuration.md.
func DefaultConfig() Config {
	return Config{Alpha: 1.0, Beta: 0.5, Gamma: 0.5, LoadBoundC: 1.25, HashBlocks: 1, VirtualNodes: 128}
}

// New builds a policy by name.
func New(name string, cfg Config) (Policy, error) {
	switch name {
	case "round_robin":
		return NewRoundRobin(), nil
	case "least_loaded":
		return NewLeastLoaded(cfg.Seed), nil
	case "consistent_hash":
		return NewConsistentHash(cfg.HashBlocks, cfg.VirtualNodes), nil
	case "relay":
		return NewRelay(cfg), nil
	default:
		return nil, fmt.Errorf("routing: unknown policy %q", name)
	}
}
