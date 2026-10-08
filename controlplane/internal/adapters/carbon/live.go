package carbon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

const ESOEndpoint = "https://api.carbonintensity.org.uk/regional"

// ESO reads current regional estimates, not metered emissions or our own
// forecast. Endpoint and client are injected so failures can be tested offline.
type ESO struct {
	Endpoint string
	Client   *http.Client
}

func (p ESO) Fetch(ctx context.Context, now time.Time) ([]domain.CarbonSnapshot, error) {
	if p.Client == nil {
		return nil, fmt.Errorf("ESO HTTP client is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.Endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ESO request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ESO HTTP status %d", resp.StatusCode)
	}
	const maxBytes = 1 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxBytes {
		return nil, fmt.Errorf("ESO response exceeds %d bytes", maxBytes)
	}
	var body struct {
		Data []struct {
			From    string `json:"from"`
			To      string `json:"to"`
			Regions []struct {
				ID        int `json:"regionid"`
				Intensity struct {
					Forecast *float64 `json:"forecast"`
				} `json:"intensity"`
			} `json:"regions"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("ESO JSON: %w", err)
	}
	if len(body.Data) != 1 {
		return nil, fmt.Errorf("ESO expected one current interval")
	}
	row := body.Data[0]
	// The API uses minute precision with a UTC suffix, unlike RFC3339 seconds.
	from, err := time.Parse("2006-01-02T15:04Z", row.From)
	if err != nil {
		return nil, fmt.Errorf("ESO interval start: %w", err)
	}
	to, err := time.Parse("2006-01-02T15:04Z", row.To)
	if err != nil || to.Sub(from) != 30*time.Minute {
		return nil, fmt.Errorf("ESO expected a half-hour interval")
	}
	if from.After(now) || now.Sub(from) > 2*time.Hour {
		return nil, fmt.Errorf("ESO interval is future or stale")
	}
	regions := map[int]string{1: "gb-north-scotland", 8: "gb-west-midlands", 13: "gb-london"}
	seen := map[int]bool{}
	readings := make([]domain.CarbonSnapshot, 0, len(regions))
	for _, r := range row.Regions {
		name, wanted := regions[r.ID]
		if !wanted {
			continue
		}
		if seen[r.ID] || r.Intensity.Forecast == nil {
			return nil, fmt.Errorf("ESO missing or duplicate region %d reading", r.ID)
		}
		value := *r.Intensity.Forecast
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return nil, fmt.Errorf("ESO invalid region %d intensity", r.ID)
		}
		seen[r.ID] = true
		readings = append(readings, domain.CarbonSnapshot{Region: name, At: from,
			GramsPerKWh: value, Source: "eso-regional-estimate", ObservedAt: now.UTC()})
	}
	if len(readings) != len(regions) {
		return nil, fmt.Errorf("ESO response lacks configured GB regions")
	}
	sort.Slice(readings, func(i, j int) bool { return readings[i].Region < readings[j].Region })
	return readings, nil
}
