package status

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// The complete set of JSON keys the public page may publish. Adding a field
// to the DTO means adding it here, in a reviewed diff.
var allowlist = []string{
	"schema", "generated_at", "evidence", "pools", "queue",
	"pools[].pool", "pools[].region", "pools[].workers_ready", "pools[].gpus_total", "pools[].gpus_reserved",
	"pools[].utilization", "pools[].heartbeat_fresh", "pools[].suppressed", "pools[].outcomes_7d",
	"pools[].pilot_jobs_7d", "pools[].real_jobs_7d",
	"pools[].outcomes_7d.succeeded", "pools[].outcomes_7d.failed", "pools[].outcomes_7d.cancelled", "pools[].outcomes_7d.expired",
	"queue.queued_jobs", "queue.queued_pilot", "queue.queued_real",
}

func jsonKeys(t reflect.Type, prefix string, out *[]string) {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
		if t.Kind() == reflect.Slice {
			prefix += "[]"
		}
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || t == reflect.TypeOf(time.Time{}) {
		return
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		key := name
		if prefix != "" {
			key = prefix + "." + name
		}
		*out = append(*out, key)
		jsonKeys(f.Type, key, out)
	}
}

func TestPublicDTOIsExactlyTheAllowlist(t *testing.T) {
	var got []string
	jsonKeys(reflect.TypeOf(PublicStatusV1{}), "", &got)
	sort.Strings(got)
	want := append([]string(nil), allowlist...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("public DTO keys changed\n got: %v\nwant: %v", got, want)
	}
	for _, k := range got {
		leaf := k[strings.LastIndex(k, ".")+1:]
		for _, bad := range []string{"id", "tenant", "user", "command", "image", "token", "host", "worker", "name", "email"} {
			if leaf == bad || strings.HasSuffix(leaf, "_"+bad) || strings.HasPrefix(leaf, bad+"_") {
				t.Errorf("public key %q looks like an identifier (%s)", k, bad)
			}
		}
	}
}

func TestRoutesAreGETOnly(t *testing.T) {
	s := New(fakeSource{}, time.Second, time.Now, nil)
	if len(s.Routes) == 0 {
		t.Fatal("no routes")
	}
	for _, r := range s.Routes {
		if !strings.HasPrefix(r, "GET ") {
			t.Fatalf("public status route %q is not GET", r)
		}
	}
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	for _, m := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		for _, p := range []string{"/", "/status.json", "/v1/jobs"} {
			req, _ := http.NewRequest(m, srv.URL+p, strings.NewReader("{}"))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode < 400 {
				t.Fatalf("%s %s answered %d", m, p, resp.StatusCode)
			}
		}
	}
}

type fakeSource struct{ calls *int }

func (f fakeSource) Read(context.Context) ([]PoolRow, QueueRow, time.Time, error) {
	if f.calls != nil {
		*f.calls++
	}
	n := int64(4)
	hb := time.Now()
	return []PoolRow{{Pool: "b", Region: "london", GPUsTotal: 8, GPUsReserved: 2, NewestHeartbeat: &hb, Succeeded7d: &n},
		{Pool: "a", Region: "north-scotland", GPUsTotal: 0}}, QueueRow{QueuedJobs: 3}, time.Now(), nil
}

func TestBuildSuppressesAndCaches(t *testing.T) {
	calls := 0
	clock := time.Now()
	s := New(fakeSource{calls: &calls}, 5*time.Second, func() time.Time { return clock }, nil)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	var v PublicStatusV1
	for i := 0; i < 10; i++ {
		resp, err := http.Get(srv.URL + "/status.json")
		if err != nil {
			t.Fatal(err)
		}
		_ = json.NewDecoder(resp.Body).Decode(&v)
		resp.Body.Close()
	}
	if calls != 1 {
		t.Fatalf("10 readers inside the TTL caused %d reads", calls)
	}
	if v.Pools[0].Pool != "a" || !v.Pools[0].Suppressed || v.Pools[0].Utilization != nil || v.Pools[0].Outcomes7d != nil {
		t.Fatalf("pool a: %+v", v.Pools[0])
	}
	if v.Pools[1].Outcomes7d == nil || *v.Pools[1].Utilization != 0.25 {
		t.Fatalf("pool b: %+v", v.Pools[1])
	}
	resp, _ := http.Get(srv.URL + "/")
	if resp.StatusCode != 200 {
		t.Fatalf("page %d", resp.StatusCode)
	}
	resp.Body.Close()
}
