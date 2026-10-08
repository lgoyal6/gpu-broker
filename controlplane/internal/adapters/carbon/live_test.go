package carbon

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func liveResponse(from time.Time) map[string]any {
	return map[string]any{"data": []any{map[string]any{
		"from": from.Format("2006-01-02T15:04Z"), "to": from.Add(30 * time.Minute).Format("2006-01-02T15:04Z"),
		"regions": []any{
			map[string]any{"regionid": 13, "intensity": map[string]any{"forecast": 81}},
			map[string]any{"regionid": 1, "intensity": map[string]any{"forecast": 0}},
			map[string]any{"regionid": 8, "intensity": map[string]any{"forecast": 25}},
		},
	}}}
}

func TestESOPreservesTimestampSourceAndZero(t *testing.T) {
	now := time.Date(2026, 10, 8, 9, 10, 0, 0, time.UTC)
	from := now.Truncate(30 * time.Minute)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "application/json" {
			t.Error("missing accept header")
		}
		_ = json.NewEncoder(w).Encode(liveResponse(from))
	}))
	defer srv.Close()
	rs, err := (ESO{Endpoint: srv.URL, Client: srv.Client()}).Fetch(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 3 {
		t.Fatalf("readings: %v", rs)
	}
	for _, r := range rs {
		if r.At != from || r.ObservedAt != now || r.Source != "eso-regional-estimate" {
			t.Fatalf("provenance changed: %+v", r)
		}
	}
	if rs[1].Region != "gb-north-scotland" || rs[1].GramsPerKWh != 0 {
		t.Fatal("zero was treated as missing")
	}
}

func TestESORejectsInvalidResponses(t *testing.T) {
	now := time.Date(2026, 10, 8, 9, 10, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"missing data", func(b map[string]any) { delete(b, "data") }},
		{"stale", func(b map[string]any) { b["data"] = liveResponse(now.Add(-3 * time.Hour))["data"] }},
		{"future", func(b map[string]any) { b["data"] = liveResponse(now.Add(time.Hour))["data"] }},
		{"invalid timestamp", func(b map[string]any) { b["data"].([]any)[0].(map[string]any)["from"] = "yesterday" }},
		{"invalid interval", func(b map[string]any) { b["data"].([]any)[0].(map[string]any)["to"] = "2026-10-08T08:30Z" }},
		{"missing region", func(b map[string]any) {
			row := b["data"].([]any)[0].(map[string]any)
			row["regions"] = row["regions"].([]any)[:2]
		}},
		{"duplicate region", func(b map[string]any) {
			row := b["data"].([]any)[0].(map[string]any)
			regs := row["regions"].([]any)
			row["regions"] = append(regs, regs[0])
		}},
		{"null intensity", func(b map[string]any) {
			b["data"].([]any)[0].(map[string]any)["regions"].([]any)[0].(map[string]any)["intensity"] = map[string]any{"forecast": nil}
		}},
		{"negative intensity", func(b map[string]any) {
			b["data"].([]any)[0].(map[string]any)["regions"].([]any)[0].(map[string]any)["intensity"] = map[string]any{"forecast": -1}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := liveResponse(now.Truncate(30 * time.Minute))
			tc.mutate(b)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(b) }))
			defer srv.Close()
			if rs, err := (ESO{Endpoint: srv.URL, Client: srv.Client()}).Fetch(context.Background(), now); err == nil || rs != nil {
				t.Fatalf("accepted invalid response: %v %v", rs, err)
			}
		})
	}
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"http failure", "unavailable", 503}, {"malformed", "{", 200},
		{"oversized", strings.Repeat(" ", (1<<20)+1), 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			if _, err := (ESO{Endpoint: srv.URL, Client: srv.Client()}).Fetch(context.Background(), now); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestESOCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (ESO{Endpoint: ESOEndpoint, Client: http.DefaultClient}).Fetch(ctx, time.Now())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
