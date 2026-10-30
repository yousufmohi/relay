// Package config reads the router's configuration from environment variables
// (names match docs/configuration.md).
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Listen   string // HTTP listen address
	RouterID string
	APIKey   string // optional; if set, require "Authorization: Bearer <key>"

	Policy     string
	BlockSize  int
	Alpha      float64
	Beta       float64
	Gamma      float64
	LoadBoundC float64
	LoadFloor  int
	HashBlocks int

	QueueMax             int
	MaxInflightPerWorker int
	QueueDeadline        time.Duration // default max time a request may wait to be dispatched

	StallTimeout      time.Duration
	FirstTokenTimeout time.Duration
	MaxFailovers      int

	// Worker discovery: exactly one of these is used, in this order.
	Workers     string // static list: id=url|sidecar,...
	WorkersFile string // same format, re-read every second
	WorkerDNS   string // headless Service name
	WorkerPort  int
	SidecarPort int

	WorkerFlavor     string // llamacpp | vllm
	WorkerModel      string
	ChatTemplate     string // chatml | llama3
	Tokenizer        string // remote | approx
	IndexCapacity    int    // default blocks per worker index before sidecar reports
	SidecarTimeout   time.Duration
	HealthInterval   time.Duration
	BreakerThreshold int
	BreakerCooldown  time.Duration
}

func Load() (Config, error) {
	host, _ := os.Hostname()
	c := Config{
		Listen:   str("LISTEN_ADDR", ":8080"),
		RouterID: str("ROUTER_ID", host),
		APIKey:   str("API_KEY", ""),

		Policy:     str("POLICY", "relay"),
		BlockSize:  num("BLOCK_SIZE_TOKENS", 64),
		Alpha:      flt("SCORE_ALPHA", 1.0),
		Beta:       flt("SCORE_BETA", 0.5),
		Gamma:      flt("SCORE_GAMMA", 0.5),
		LoadBoundC: flt("LOAD_BOUND_C", 1.25),
		LoadFloor:  num("LOAD_BOUND_FLOOR", 0),
		HashBlocks: num("CONSISTENT_HASH_BLOCKS", 1),

		QueueMax:             num("QUEUE_MAX", 256),
		MaxInflightPerWorker: num("MAX_INFLIGHT_PER_WORKER", 8),
		QueueDeadline:        ms("QUEUE_DEADLINE_MS", 30000),

		StallTimeout:      ms("STALL_TIMEOUT_MS", 3000),
		FirstTokenTimeout: ms("FIRST_TOKEN_TIMEOUT_MS", 30000),
		MaxFailovers:      num("MAX_FAILOVERS", 2),

		Workers:     str("WORKERS", ""),
		WorkersFile: str("WORKERS_FILE", ""),
		WorkerDNS:   str("WORKER_DNS", ""),
		WorkerPort:  num("WORKER_PORT", 8080),
		SidecarPort: num("SIDECAR_PORT", 9500),

		WorkerFlavor:     str("WORKER_FLAVOR", "llamacpp"),
		WorkerModel:      str("WORKER_MODEL", ""),
		ChatTemplate:     str("CHAT_TEMPLATE", "chatml"),
		Tokenizer:        str("TOKENIZER", "remote"),
		IndexCapacity:    num("INDEX_DEFAULT_BLOCKS", 512),
		SidecarTimeout:   ms("SIDECAR_TIMEOUT_MS", 1500),
		HealthInterval:   ms("HEALTH_INTERVAL_MS", 1000),
		BreakerThreshold: num("BREAKER_THRESHOLD", 3),
		BreakerCooldown:  ms("BREAKER_COOLDOWN_MS", 5000),
	}
	if c.Workers == "" && c.WorkersFile == "" && c.WorkerDNS == "" {
		return c, fmt.Errorf("config: set one of WORKERS, WORKERS_FILE or WORKER_DNS")
	}
	if c.BlockSize <= 0 {
		return c, fmt.Errorf("config: BLOCK_SIZE_TOKENS must be positive")
	}
	return c, nil
}

func str(k, d string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return d
}

func num(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func flt(k string, d float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
		return v
	}
	return d
}

func ms(k string, d int) time.Duration { return time.Duration(num(k, d)) * time.Millisecond }
