package spot

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type gceRoundTripper func(*http.Request) (*http.Response, error)

func (f gceRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func gceClient(body string, status int, seen *bool) *http.Client {
	return &http.Client{Transport: gceRoundTripper(func(r *http.Request) (*http.Response, error) {
		*seen = r.URL.String() == gcePreemptedURL && r.Header.Get("Metadata-Flavor") == "Google"
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
}

func TestGCEPreemptionMetadata(t *testing.T) {
	for _, tc := range []struct {
		body string
		want bool
	}{{"FALSE", false}, {"TRUE", true}} {
		seen := false
		got, err := (GCE{Client: gceClient(tc.body, http.StatusOK, &seen)}).Interrupted(context.Background(), time.Now())
		if err != nil || got != tc.want || !seen {
			t.Fatalf("body=%s got=%v err=%v request-valid=%v", tc.body, got, err, seen)
		}
	}
}

func TestGCEPreemptionMetadataRejectsBadResponses(t *testing.T) {
	for _, tc := range []struct {
		body   string
		status int
	}{{"TRUE-ish", 200}, {"FALSE", 503}} {
		seen := false
		if got, err := (GCE{Client: gceClient(tc.body, tc.status, &seen)}).Interrupted(context.Background(), time.Now()); err == nil || got {
			t.Fatalf("body=%s status=%d got=%v err=%v", tc.body, tc.status, got, err)
		}
	}
	client := &http.Client{Transport: gceRoundTripper(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") })}
	if got, err := (GCE{Client: client}).Interrupted(context.Background(), time.Now()); err == nil || got {
		t.Fatalf("offline got=%v err=%v", got, err)
	}
}
