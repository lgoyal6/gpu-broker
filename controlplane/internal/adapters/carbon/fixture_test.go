package carbon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const fixture = "../../../data/carbon/gb-2026-08-03_2026-09-28.json"

func TestFixtureLoadsAndVerifiesItsChecksum(t *testing.T) {
	d, err := Load(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.NationalActual) != 2688 || len(d.Regional) != 3 || d.Step != 30*time.Minute {
		t.Fatalf("shape: %d national, %d regions", len(d.NationalActual), len(d.Regional))
	}
	raw, _ := os.ReadFile(fixture)
	// Change one value: the loader must refuse it.
	tampered := strings.Replace(string(raw), `"gb-london":[`, `"gb-london":[999,`, 1)
	p := filepath.Join(t.TempDir(), "t.json")
	_ = os.WriteFile(p, []byte(tampered), 0o600)
	if _, err := Load(p); err == nil || !strings.Contains(err.Error(), "does not match manifest") {
		t.Fatalf("tampered fixture loaded: %v", err)
	}
}

func TestReplayIsRelabelled(t *testing.T) {
	d, err := Load(fixture)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 6, 12, 7, 0, 0, time.UTC)
	ss := d.Snapshots("gb-london", "london", &now, now)
	last := ss[len(ss)-1]
	if !last.At.Equal(now.Truncate(30*time.Minute).Add(-30*time.Minute)) || !strings.HasPrefix(last.Source, "replay:") || last.Region != "london" {
		t.Fatalf("replay: %+v", last)
	}
	if ss := d.Snapshots("gb-london", "london", nil, now); ss[0].Source != SourceRegional {
		t.Fatalf("plain load source %s", ss[0].Source)
	}
}
