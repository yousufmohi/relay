package internal

import (
	"testing"
	"time"
)

func targets() Targets {
	return Targets{QueuePerWorker: 4, TTFTP95: 800 * time.Millisecond, Min: 2, Max: 8,
		UpWindow: 20 * time.Second, UpCooldown: 30 * time.Second, DownWindow: 60 * time.Second}
}

var t0 = time.Unix(1000, 0)

func at(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

func TestBounds(t *testing.T) {
	d := &Decider{T: targets()}
	if got := d.Decide(Observation{Now: t0, Replicas: 1}); got.Desired != 2 || got.Direction != "up" {
		t.Fatalf("%+v", got)
	}
	if got := d.Decide(Observation{Now: t0, Replicas: 10}); got.Desired != 8 {
		t.Fatalf("%+v", got)
	}
}

func TestScaleUpNeedsSustainedOverload(t *testing.T) {
	d := &Decider{T: targets()}
	hot := func(s int) Decision { return d.Decide(Observation{Now: at(s), Replicas: 2, QueueDepth: 20}) }
	if got := hot(0); got.Direction != "none" {
		t.Fatalf("instant spike must not scale: %+v", got)
	}
	if got := hot(10); got.Direction != "none" {
		t.Fatal("still inside window")
	}
	got := hot(20)
	if got.Direction != "up" || got.Desired != 4 { // ratio 2.5 capped at 2× → 4
		t.Fatalf("%+v", got)
	}
}

func TestScaleUpWindowResetsWhenLoadDrops(t *testing.T) {
	d := &Decider{T: targets()}
	d.Decide(Observation{Now: at(0), Replicas: 2, QueueDepth: 20})
	d.Decide(Observation{Now: at(10), Replicas: 2, QueueDepth: 6}) // ratio 0.75: within targets
	if got := d.Decide(Observation{Now: at(25), Replicas: 2, QueueDepth: 20}); got.Direction != "none" {
		t.Fatalf("window must restart: %+v", got)
	}
}

func TestTTFTAloneTriggersScaleUp(t *testing.T) {
	d := &Decider{T: targets()}
	d.Decide(Observation{Now: at(0), Replicas: 3, TTFTP95: 1200 * time.Millisecond})
	got := d.Decide(Observation{Now: at(20), Replicas: 3, TTFTP95: 1200 * time.Millisecond})
	if got.Direction != "up" || got.Desired != 5 { // ceil(3×1.5)
		t.Fatalf("%+v", got)
	}
}

func TestUpCooldownAndMax(t *testing.T) {
	d := &Decider{T: targets()}
	d.Decide(Observation{Now: at(0), Replicas: 6, QueueDepth: 100})
	if got := d.Decide(Observation{Now: at(20), Replicas: 6, QueueDepth: 100}); got.Desired != 8 {
		t.Fatalf("capped at max: %+v", got)
	}
	d.Decide(Observation{Now: at(21), Replicas: 7, QueueDepth: 100})
	if got := d.Decide(Observation{Now: at(45), Replicas: 7, QueueDepth: 100}); got.Direction != "none" {
		t.Fatalf("cooldown: %+v", got)
	}
}

func TestScaleDownOneAtATimeAfterLongWindow(t *testing.T) {
	d := &Decider{T: targets()}
	idle := func(s, r int) Decision { return d.Decide(Observation{Now: at(s), Replicas: r}) }
	if got := idle(0, 5); got.Direction != "none" {
		t.Fatal("first idle reading")
	}
	if got := idle(59, 5); got.Direction != "none" {
		t.Fatal("inside down window")
	}
	if got := idle(60, 5); got.Direction != "down" || got.Desired != 4 {
		t.Fatalf("%+v", got)
	}
	if got := idle(90, 4); got.Direction != "none" {
		t.Fatal("must wait another window before the next step")
	}
	if got := idle(200, 4); got.Desired != 3 {
		t.Fatalf("%+v", got)
	}
}

func TestNoScaleDownBelowMin(t *testing.T) {
	d := &Decider{T: targets()}
	d.Decide(Observation{Now: at(0), Replicas: 2})
	if got := d.Decide(Observation{Now: at(500), Replicas: 2}); got.Direction != "none" {
		t.Fatalf("%+v", got)
	}
}

func TestNoScaleDownRightAfterScaleUp(t *testing.T) {
	d := &Decider{T: targets()}
	d.Decide(Observation{Now: at(0), Replicas: 2, QueueDepth: 40})
	d.Decide(Observation{Now: at(20), Replicas: 2, QueueDepth: 40}) // up
	d.Decide(Observation{Now: at(21), Replicas: 4})
	if got := d.Decide(Observation{Now: at(70), Replicas: 4}); got.Direction != "none" {
		t.Fatalf("flapping: %+v", got)
	}
}
