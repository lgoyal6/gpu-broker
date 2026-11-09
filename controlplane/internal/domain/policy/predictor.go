package policy

import (
	"math"
	"sort"
	"time"
)

// RuntimeObservation is an outcome that was durably known before a decision.
// Family and device identify a workload class without exposing its command.
type RuntimeObservation struct {
	Family   string
	Device   string
	Started  time.Time
	Finished time.Time
}

type RuntimeEstimate struct {
	Seconds       float64 `json:"seconds"`
	P50Seconds    float64 `json:"p50_seconds"`
	P90Seconds    float64 `json:"p90_seconds"`
	Samples       int     `json:"samples"`
	RelativeWidth float64 `json:"relative_width"`
	Source        string  `json:"source"`
}

// EstimateRuntime uses only completed observations visible at asOf. Small or
// empty cohorts use a caller supplied conservative cold-start bound.
func EstimateRuntime(family, device string, asOf time.Time, observations []RuntimeObservation,
	minimumSamples int, coldStart time.Duration) RuntimeEstimate {
	if minimumSamples < 1 {
		minimumSamples = 1
	}
	values := make([]float64, 0, len(observations))
	for _, observation := range observations {
		if observation.Family != family || observation.Device != device ||
			observation.Started.IsZero() || observation.Finished.IsZero() ||
			observation.Started.After(asOf) || observation.Finished.After(asOf) ||
			!observation.Finished.After(observation.Started) {
			continue
		}
		values = append(values, observation.Finished.Sub(observation.Started).Seconds())
	}
	if len(values) < minimumSamples {
		seconds := coldStart.Seconds()
		if seconds < 0 {
			seconds = 0
		}
		return RuntimeEstimate{Seconds: seconds, P50Seconds: seconds, P90Seconds: seconds,
			Samples: len(values), RelativeWidth: 0, Source: "cold-start-bound"}
	}
	sort.Float64s(values)
	p50 := quantile(values, 0.50)
	p90 := quantile(values, 0.90)
	return RuntimeEstimate{Seconds: p90, P50Seconds: p50, P90Seconds: p90,
		Samples: len(values), RelativeWidth: math.Max(0, (p90-p50)/math.Max(p50, 1e-9)),
		Source: "historical-p90"}
}

func quantile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(q*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}
