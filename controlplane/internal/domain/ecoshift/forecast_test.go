package ecoshift

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

var base = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

// diurnal is a clean daily cycle: the seasonal model should nail it and
// persistence should not.
func diurnal(days int, noise func(i int) float64) []Reading {
	var rs []Reading
	for i := 0; i < days*48; i++ {
		t := base.Add(time.Duration(i) * Step)
		h := float64(t.Hour()) + float64(t.Minute())/60
		rs = append(rs, Reading{At: t, GramsPerKWh: 150 + 80*math.Sin(2*math.Pi*h/24) + noise(i)})
	}
	return rs
}

func TestSeasonalBeatsPersistenceOnADailyCycle(t *testing.T) {
	s := NewSeries(diurnal(14, func(int) float64 { return 0 }))
	bt := s.Backtest(base.Add(8*24*time.Hour), base.Add(13*24*time.Hour), 6*time.Hour, 24*time.Hour)
	if bt[SeasonalModel].MAE > 1e-9 {
		t.Fatalf("seasonal MAE on a perfect cycle: %v", bt[SeasonalModel].MAE)
	}
	if bt["persistence"].MAE < 10 {
		t.Fatalf("persistence should be poor on a cycle: %v", bt["persistence"].MAE)
	}
	if bt[SeasonalModel].N != bt["persistence"].N || bt["persistence"].N != bt["same-time-yesterday"].N {
		t.Fatalf("models scored on different points: %+v", bt)
	}
}

func TestTrustedForecastBandsAndRefusal(t *testing.T) {
	hist := diurnal(10, func(i int) float64 { return float64(i%7) - 3 })
	now := base.Add(10 * 24 * time.Hour)
	fc, model, ok := TrustedForecast(hist, now, 24*time.Hour)
	if !ok || model != SeasonalModel || len(fc) != 48 {
		t.Fatalf("expected a trusted 48-point forecast, got ok=%v n=%d", ok, len(fc))
	}
	for _, p := range fc {
		if !(p.Low <= p.GramsPerKWh && p.GramsPerKWh <= p.High) || p.High-p.GramsPerKWh <= 0 {
			t.Fatalf("bad band %+v", p)
		}
	}
	// A random walk has no daily cycle; persistence is the right model, so
	// the seasonal one must not be trusted.
	r := rand.New(rand.NewPCG(5, 6))
	var flat []Reading
	v := 200.0
	for i := 0; i < 10*48; i++ {
		v = math.Max(0, v+r.NormFloat64()*15)
		flat = append(flat, Reading{At: base.Add(time.Duration(i) * Step), GramsPerKWh: v})
	}
	if _, _, ok := TrustedForecast(flat, now, 24*time.Hour); ok {
		t.Fatal("trusted a seasonal forecast that does not beat persistence")
	}
	// Too little history.
	if _, _, ok := TrustedForecast(hist[:48*2], base.Add(2*24*time.Hour), 24*time.Hour); ok {
		t.Fatal("trusted a forecast with two days of history")
	}
}

func TestSeasonalNeverReadsTheFuture(t *testing.T) {
	s := NewSeries(diurnal(14, func(i int) float64 { return float64(i) }))
	asOf := base.Add(10 * 24 * time.Hour)
	target := asOf.Add(12 * time.Hour)
	got, ok := s.Seasonal(target, asOf)
	if !ok {
		t.Fatal("no prediction")
	}
	// Mutating data after asOf must not change the prediction.
	for k := range s {
		if k.After(asOf) {
			s[k] = 1e9
		}
	}
	again, _ := s.Seasonal(target, asOf)
	if got != again {
		t.Fatalf("prediction used data after asOf: %v vs %v", got, again)
	}
}

func TestEnergyAndFreshness(t *testing.T) {
	kwh, ok := EnergyKWh("a100", 2, 3*time.Hour, 1.2)
	if !ok || math.Abs(kwh-2.88) > 1e-9 {
		t.Fatalf("a100 x2 x3h x1.2: %v", kwh)
	}
	if _, ok := EnergyKWh("mystery-gpu", 1, time.Hour, 1); ok {
		t.Fatal("unknown model produced an energy figure")
	}
	now := base
	rc := RegionCarbon{Current: &Reading{At: now.Add(-90 * time.Minute)}}
	if !rc.Fresh(now, 2*time.Hour) || rc.Fresh(now, time.Hour) {
		t.Fatal("freshness boundary")
	}
	if (RegionCarbon{}).Fresh(now, time.Hour) {
		t.Fatal("missing reading counted as fresh")
	}
}
