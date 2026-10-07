package ecoshift

import (
	"math"
	"sort"
	"time"
)

// Step is the interval of the carbon series (GB ESO publishes half-hours).
const Step = 30 * time.Minute

// SeasonalModel names the forecaster and its version. Bump the version when
// the arithmetic changes; decisions record it.
const SeasonalModel = "seasonal-7d@v1"

// Series is readings keyed by interval start, one value per interval.
type Series map[time.Time]float64

// NewSeries collapses readings to one value per interval (the last one wins,
// readings arrive in time-then-source order).
func NewSeries(rs []Reading) Series {
	s := Series{}
	for _, r := range rs {
		s[r.At.UTC().Truncate(Step)] = r.GramsPerKWh
	}
	return s
}

// Seasonal predicts t as the mean of the same half-hour on the previous seven
// days, using only data at or before `asOf`. It needs at least three of the
// seven days; otherwise ok is false.
func (s Series) Seasonal(t, asOf time.Time) (float64, bool) {
	sum, n := 0.0, 0
	for k := 1; k <= 7; k++ {
		at := t.Add(-time.Duration(k) * 24 * time.Hour)
		if at.After(asOf) {
			continue
		}
		if v, ok := s[at]; ok {
			sum += v
			n++
		}
	}
	if n < 3 {
		return 0, false
	}
	return sum / float64(n), true
}

// Persistence predicts every future interval as the last value at or before asOf.
func (s Series) Persistence(asOf time.Time) (float64, bool) {
	for t := asOf.Truncate(Step); t.After(asOf.Add(-6 * time.Hour)); t = t.Add(-Step) {
		if v, ok := s[t]; ok {
			return v, true
		}
	}
	return 0, false
}

// Yesterday predicts t as the value 24h earlier.
func (s Series) Yesterday(t, asOf time.Time) (float64, bool) {
	at := t.Add(-24 * time.Hour)
	if at.After(asOf) {
		return 0, false
	}
	v, ok := s[at]
	return v, ok
}

// Errors is a forecast's error over a set of points.
type Errors struct {
	N         int     `json:"n"`
	MAE       float64 `json:"mae_g_per_kwh"`
	MAPE      float64 `json:"mape_percent"`
	P90AbsErr float64 `json:"p90_abs_error_g_per_kwh"`
}

func ErrorsOf(pred, actual []float64) Errors {
	if len(pred) == 0 {
		return Errors{}
	}
	abs := make([]float64, len(pred))
	sum, sumPct, nPct := 0.0, 0.0, 0
	for i := range pred {
		abs[i] = math.Abs(pred[i] - actual[i])
		sum += abs[i]
		if actual[i] > 0 {
			sumPct += abs[i] / actual[i]
			nPct++
		}
	}
	sort.Float64s(abs)
	e := Errors{N: len(pred), MAE: sum / float64(len(pred)), P90AbsErr: abs[int(math.Ceil(0.9*float64(len(abs))))-1]}
	if nPct > 0 {
		e.MAPE = 100 * sumPct / float64(nPct)
	}
	return e
}

// Backtest evaluates day-ahead forecasts issued at each anchor in
// [from, to) every `every`, over `horizon`, for the three models. Every model
// is scored on exactly the same (anchor, target) pairs: a pair is used only
// if all models produced a prediction and the actual exists.
func (s Series) Backtest(from, to time.Time, every, horizon time.Duration) map[string]Errors {
	preds := map[string][]float64{}
	var actual []float64
	for anchor := from; anchor.Before(to); anchor = anchor.Add(every) {
		p, okP := s.Persistence(anchor)
		for t := anchor.Add(Step); !t.After(anchor.Add(horizon)); t = t.Add(Step) {
			a, okA := s[t]
			se, okS := s.Seasonal(t, anchor)
			y, okY := s.Yesterday(t, anchor)
			if !(okA && okP && okS && okY) {
				continue
			}
			actual = append(actual, a)
			preds["persistence"] = append(preds["persistence"], p)
			preds[SeasonalModel] = append(preds[SeasonalModel], se)
			preds["same-time-yesterday"] = append(preds["same-time-yesterday"], y)
		}
	}
	out := map[string]Errors{}
	for k, v := range preds {
		out[k] = ErrorsOf(v, actual)
	}
	return out
}

// TrustedForecast returns a seasonal forecast for (now, now+horizon] only if,
// on the last two days of history, it beat persistence. The band is the
// backtest's 90th-percentile absolute error, so the delay rule's "pessimistic
// value" is grounded in how wrong this model has actually been here.
func TrustedForecast(hist []Reading, now time.Time, horizon time.Duration) ([]ForecastPoint, string, bool) {
	s := NewSeries(hist)
	base := now.UTC().Truncate(Step)
	bt := s.Backtest(base.Add(-48*time.Hour), base.Add(-horizon), 6*time.Hour, horizon)
	seas, pers := bt[SeasonalModel], bt["persistence"]
	if seas.N < 48 || seas.MAE >= pers.MAE {
		return nil, "", false
	}
	var out []ForecastPoint
	for t := base.Add(Step); !t.After(base.Add(horizon)); t = t.Add(Step) {
		v, ok := s.Seasonal(t, base)
		if !ok {
			continue
		}
		out = append(out, ForecastPoint{At: t, GramsPerKWh: v, Low: math.Max(0, v-seas.P90AbsErr), High: v + seas.P90AbsErr})
	}
	return out, SeasonalModel, len(out) > 0
}
