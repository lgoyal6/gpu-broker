package spot

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const gcePreemptedURL = "http://metadata.google.internal/computeMetadata/v1/instance/preempted"

type GCE struct{ Client *http.Client }

func (p GCE) Interrupted(ctx context.Context, _ time.Time) (bool, error) {
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gcePreemptedURL, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("GCE preemption metadata: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("GCE preemption metadata returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 17))
	if err != nil {
		return false, err
	}
	if len(body) > 16 {
		return false, fmt.Errorf("GCE preemption metadata response too large")
	}
	switch strings.TrimSpace(string(body)) {
	case "TRUE":
		return true, nil
	case "FALSE":
		return false, nil
	default:
		return false, fmt.Errorf("unexpected GCE preemption metadata value")
	}
}
