// Package carbon loads versioned carbon-intensity datasets (ADR 0006).
package carbon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
)

type Manifest struct {
	Dataset       string            `json:"dataset"`
	Source        string            `json:"source"`
	SourceURL     string            `json:"source_url"`
	RetrievedAt   string            `json:"retrieved_at"`
	NationalBasis string            `json:"national_basis"`
	RegionalBasis string            `json:"regional_basis"`
	Regions       map[string]string `json:"regions"`
	Unit          string            `json:"unit"`
	SHA256        string            `json:"sha256"`
}

type data struct {
	Start       string `json:"start"`
	StepMinutes int    `json:"step_minutes"`
	Points      int    `json:"points"`
	National    struct {
		Actual      []*float64 `json:"actual"`
		ESOForecast []*float64 `json:"eso_forecast"`
	} `json:"national"`
	Regional map[string][]*float64 `json:"regional_estimate"`
}

// Dataset is a loaded, checksum-verified fixture.
type Dataset struct {
	Manifest       Manifest
	Start          time.Time
	Step           time.Duration
	NationalActual []ecoshift.Reading
	ESOForecast    []ecoshift.Reading
	Regional       map[string][]ecoshift.Reading
}

const (
	SourceRegional = "eso-regional-estimate"
	SourceNational = "eso-national-actual"
)

// Load refuses a dataset whose data does not hash to its manifest: a
// hand-edited fixture would otherwise quietly change every result built on it.
func Load(path string) (*Dataset, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Manifest Manifest        `json:"manifest"`
		Data     json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	// The producer hashed json.dumps(data, sort_keys=True) with Python's
	// default separators; re-serialise the same way to verify.
	var generic any
	if err := json.Unmarshal(doc.Data, &generic); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(pyDumps(generic)))
	if got := hex.EncodeToString(sum[:]); got != doc.Manifest.SHA256 {
		return nil, fmt.Errorf("dataset %s: data sha256 %s does not match manifest %s", path, got[:12], doc.Manifest.SHA256)
	}
	var d data
	if err := json.Unmarshal(doc.Data, &d); err != nil {
		return nil, err
	}
	start, err := time.Parse("2006-01-02T15:04Z", d.Start)
	if err != nil {
		return nil, err
	}
	ds := &Dataset{Manifest: doc.Manifest, Start: start, Step: time.Duration(d.StepMinutes) * time.Minute, Regional: map[string][]ecoshift.Reading{}}
	series := func(vs []*float64, source string) []ecoshift.Reading {
		var out []ecoshift.Reading
		for i, v := range vs {
			if v == nil {
				continue // missing stays missing; never interpolated
			}
			out = append(out, ecoshift.Reading{At: start.Add(time.Duration(i) * ds.Step), GramsPerKWh: *v, Source: source})
		}
		return out
	}
	ds.NationalActual = series(d.National.Actual, SourceNational)
	ds.ESOForecast = series(d.National.ESOForecast, "eso-national-forecast")
	for name, vs := range d.Regional {
		ds.Regional[name] = series(vs, SourceRegional)
	}
	return ds, nil
}

func (d *Dataset) End() time.Time {
	end := d.Start
	for _, rs := range d.Regional {
		if len(rs) > 0 && rs[len(rs)-1].At.After(end) {
			end = rs[len(rs)-1].At
		}
	}
	return end.Add(d.Step)
}

func (d *Dataset) RegionNames() []string {
	var out []string
	for k := range d.Regional {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Snapshots converts a region's series for storage. With replayAt set, the
// series is shifted so it ends at replayAt and the source is relabelled
// "replay:<dataset>": a Kind or local demo then has "current" readings, and
// every decision built on them says they are replayed history, not live data.
func (d *Dataset) Snapshots(region, as string, replayAt *time.Time, observedAt time.Time) []domain.CarbonSnapshot {
	rs := d.Regional[region]
	var shift time.Duration
	source := SourceRegional
	if replayAt != nil {
		shift = replayAt.Truncate(d.Step).Sub(d.End())
		source = "replay:" + d.Manifest.Dataset
	}
	out := make([]domain.CarbonSnapshot, 0, len(rs))
	for _, r := range rs {
		out = append(out, domain.CarbonSnapshot{Region: as, At: r.At.Add(shift), GramsPerKWh: r.GramsPerKWh, Source: source, ObservedAt: observedAt})
	}
	return out
}
