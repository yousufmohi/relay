package internal

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestQuantileFromBucketDeltas(t *testing.T) {
	old := map[float64]float64{0.1: 10, 0.5: 10, 1: 10, math.Inf(1): 10}
	// 100 new observations: 50 under 0.1s, 40 in (0.1,0.5], 10 in (0.5,1].
	cur := map[float64]float64{0.1: 60, 0.5: 100, 1: 110, math.Inf(1): 110}
	got := quantile(0.95, old, cur)
	// rank 95 falls in (0.5,1]: 90 below, 10 inside → 0.5 + 0.5×(5/10) = 0.75s
	if got != 750*time.Millisecond {
		t.Fatalf("got %v", got)
	}
	if quantile(0.95, cur, cur) != 0 {
		t.Fatal("no new observations → 0")
	}
}

func TestParseText(t *testing.T) {
	s, err := parseText(strings.NewReader("# c\nrelay_queue_depth 3\nrelay_ttft_seconds_bucket{policy=\"relay\",le=\"0.5\"} 7\n"))
	if err != nil || len(s) != 2 || s[1].labels["le"] != "0.5" || s[1].value != 7 || s[0].value != 3 {
		t.Fatalf("%+v %v", s, err)
	}
}
