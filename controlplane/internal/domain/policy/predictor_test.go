package policy

import (
	"testing"
	"time"
)

func TestEstimateRuntimeUsesOnlyCompletedPastCohort(t *testing.T) {
	asOf := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	start := asOf.Add(-10 * time.Hour)
	observations := []RuntimeObservation{
		{Family: "resnet", Device: "t4", Started: start, Finished: start.Add(time.Minute)},
		{Family: "resnet", Device: "t4", Started: start, Finished: start.Add(2 * time.Minute)},
		{Family: "resnet", Device: "t4", Started: start, Finished: start.Add(3 * time.Minute)},
		// This outcome is from after the decision time and must not leak.
		{Family: "resnet", Device: "t4", Started: asOf, Finished: asOf.Add(time.Hour)},
		{Family: "other", Device: "t4", Started: start, Finished: start.Add(time.Hour)},
	}
	estimate := EstimateRuntime("resnet", "t4", asOf, observations, 3, 5*time.Minute)
	if estimate.Source != "historical-p90" || estimate.Samples != 3 {
		t.Fatalf("unexpected cohort: %+v", estimate)
	}
	if estimate.P50Seconds != 120 || estimate.P90Seconds != 180 || estimate.Seconds != 180 {
		t.Fatalf("unexpected p50/p90 estimate: %+v", estimate)
	}
}

func TestEstimateRuntimeUsesColdStartUntilCohortIsLargeEnough(t *testing.T) {
	asOf := time.Now().UTC()
	observations := []RuntimeObservation{{Family: "x", Device: "a", Started: asOf.Add(-time.Hour), Finished: asOf}}
	estimate := EstimateRuntime("x", "a", asOf, observations, 2, 7*time.Minute)
	if estimate.Source != "cold-start-bound" || estimate.Samples != 1 || estimate.Seconds != 420 {
		t.Fatalf("unexpected cold-start estimate: %+v", estimate)
	}
}
