// Package ecoshift holds the carbon and cost arithmetic: energy estimates,
// carbon readings, staleness, and the forecasting models. It is pure; the
// providers that fetch readings live in adapters.
package ecoshift

import (
	"time"
)

// boardPowerWatts is the vendor-specified maximum board power per GPU. It is
// an upper bound on draw, not a measurement, so every carbon figure computed
// from it is labelled EnergyBasis.
var boardPowerWatts = map[string]float64{
	"t4":    70,
	"l4":    72,
	"a10g":  150,
	"a6000": 300,
	"l40s":  350,
	"a100":  400,
	"h100":  700,
	"vgpu":  0, // Kind's declared virtual GPUs draw nothing real
}

// EnergyBasis labels how an energy figure was produced.
const EnergyBasis = "estimate:board-power-tdp"

// EnergyKWh is board power x GPUs x duration x PUE. ok is false when the GPU
// model has no published board power, so callers report "unknown" rather than
// inventing a number.
func EnergyKWh(model string, gpus int, d time.Duration, pue float64) (float64, bool) {
	w, ok := boardPowerWatts[model]
	if !ok {
		return 0, false
	}
	if pue < 1 {
		pue = 1
	}
	return w * float64(gpus) * d.Hours() * pue / 1000, true
}

// Reading is one carbon intensity value for a region and interval.
type Reading struct {
	At          time.Time // start of the interval the value describes
	GramsPerKWh float64
	Source      string
}

// ForecastPoint is a predicted intensity with an uncertainty band. Low and
// High bound the value; the delay rule compares against High so a delay is
// only taken when even the pessimistic forecast is better.
type ForecastPoint struct {
	At          time.Time
	GramsPerKWh float64
	Low, High   float64
}

// RegionCarbon is everything a decision knows about one region's carbon.
type RegionCarbon struct {
	Current  *Reading
	Forecast []ForecastPoint
	// ForecastModel names the model and version that produced Forecast, or is
	// empty when no forecast is trusted for this region.
	ForecastModel string
}

// Fresh reports whether the current reading exists and is no older than
// maxAge at now. A missing or stale reading means the policy must fall back.
func (r RegionCarbon) Fresh(now time.Time, maxAge time.Duration) bool {
	if r.Current == nil {
		return false
	}
	age := now.Sub(r.Current.At)
	return age >= -30*time.Minute && age <= maxAge
}
