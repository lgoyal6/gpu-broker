package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/httpapi"
)

const idempotentJobBody = `{"image":"x/y:1","command":["sim"],"gpus":1,"max_runtime_s":60}`

func mutate(api http.Handler, token, path, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Idempotency-Key", key)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	api.ServeHTTP(w, r)
	return w
}

type failReceiptStore struct {
	application.Store
	fail atomic.Bool
}

type failReceiptTx struct {
	application.Tx
	store *failReceiptStore
}

func (s *failReceiptStore) InTx(ctx context.Context, fn func(application.Tx) error) error {
	return s.Store.InTx(ctx, func(tx application.Tx) error { return fn(failReceiptTx{tx, s}) })
}

func (t failReceiptTx) PutIdempotency(ctx context.Context, rec application.IdempotencyRecord) error {
	if t.store.fail.CompareAndSwap(true, false) {
		return application.ErrUnavailable
	}
	return t.Tx.PutIdempotency(ctx, rec)
}

func TestIdempotentMutationAndReceiptCommitTogether(t *testing.T) {
	for _, path := range []string{"/v1/jobs", "/v1/users"} {
		t.Run(path, func(t *testing.T) {
			e := testkit.New(t)
			_, tenant := e.Tenant("idempotency", 100)
			body, query := idempotentJobBody, `SELECT count(*) FROM jobs`
			if path == "/v1/users" {
				body, query = `{"handle":"new-user","role":"member"}`, `SELECT count(*) FROM users WHERE handle = 'new-user'`
			}
			store := &failReceiptStore{Store: e.Store}
			store.fail.Store(true)
			e.Svc.Store = store
			api := httpapi.New(e.Svc, nil, nil, nil, false).Handler()
			failed := mutate(api, tenant.Token, path, body, "receipt-failure")
			if failed.Code != 503 || e.QueryInt(query) != 0 || e.QueryInt(`SELECT count(*) FROM idempotency_keys`) != 0 {
				t.Fatalf("receipt failure committed mutation: code=%d rows=%d", failed.Code, e.QueryInt(query))
			}
			first := mutate(api, tenant.Token, path, body, "receipt-failure")
			if first.Code != 201 || e.QueryInt(query) != 1 {
				t.Fatalf("retry: code=%d body=%s", first.Code, first.Body)
			}
			// A fresh server has no request-local state from the first call.
			restarted := httpapi.New(e.Svc, nil, nil, nil, false).Handler()
			again := mutate(restarted, tenant.Token, path, body, "receipt-failure")
			var a, b any
			e.Must(json.Unmarshal(first.Body.Bytes(), &a))
			e.Must(json.Unmarshal(again.Body.Bytes(), &b))
			if again.Code != 201 || again.Header().Get("Idempotent-Replay") != "true" || !reflect.DeepEqual(a, b) || e.QueryInt(query) != 1 {
				t.Fatalf("durable replay: code=%d body=%s", again.Code, again.Body)
			}
		})
	}
}

func TestConcurrentIdempotentMutationsExecuteOnce(t *testing.T) {
	for _, path := range []string{"/v1/jobs", "/v1/users"} {
		t.Run(path, func(t *testing.T) {
			e := testkit.New(t)
			_, tenant := e.Tenant("idempotency", 100)
			body, query := idempotentJobBody, `SELECT count(*) FROM jobs`
			if path == "/v1/users" {
				body, query = `{"handle":"new-user","role":"member"}`, `SELECT count(*) FROM users WHERE handle = 'new-user'`
			}
			api := httpapi.New(e.Svc, nil, nil, nil, false).Handler()
			start, done := make(chan struct{}), make(chan *httptest.ResponseRecorder, 8)
			for range 8 {
				go func() { <-start; done <- mutate(api, tenant.Token, path, body, "concurrent") }()
			}
			close(start)
			replays := 0
			var first any
			for range 8 {
				response := <-done
				if response.Code != 201 {
					t.Fatalf("concurrent response: code=%d body=%s", response.Code, response.Body)
				}
				var decoded any
				e.Must(json.Unmarshal(response.Body.Bytes(), &decoded))
				if first == nil {
					first = decoded
				} else if !reflect.DeepEqual(first, decoded) {
					t.Fatal("same key returned different mutations or tokens")
				}
				if response.Header().Get("Idempotent-Replay") == "true" {
					replays++
				}
			}
			if replays != 7 || e.QueryInt(query) != 1 || e.QueryInt(`SELECT count(*) FROM idempotency_keys`) != 1 {
				t.Fatalf("replays=%d mutations=%d", replays, e.QueryInt(query))
			}
		})
	}
}

func TestIdempotencyPreservesAdmissionRefusalSemantics(t *testing.T) {
	e := testkit.New(t)
	_, tenant := e.Tenant("idempotency", 0)
	api := httpapi.New(e.Svc, nil, nil, nil, false).Handler()
	e.Svc.Cfg.MaxQueuedGlobal = 0
	if response := mutate(api, tenant.Token, "/v1/jobs", idempotentJobBody, "retryable"); response.Code != 503 {
		t.Fatalf("queue refusal: %d %s", response.Code, response.Body)
	}
	e.Svc.Cfg.MaxQueuedGlobal = 20000
	if response := mutate(api, tenant.Token, "/v1/jobs", idempotentJobBody, "retryable"); response.Code != 201 {
		t.Fatalf("queue retry: %d %s", response.Code, response.Body)
	}
	e.Worker(e.Pool("pool-a", "london", 1, false), "node-1", 1)
	for i := range 2 {
		response := mutate(api, tenant.Token, "/v1/jobs", idempotentJobBody, "budget-refused")
		if response.Code != 422 || (i == 1 && response.Header().Get("Idempotent-Replay") != "true") {
			t.Fatalf("budget refusal: %d %s", response.Code, response.Body)
		}
	}
	if e.QueryInt(`SELECT count(*) FROM jobs WHERE state = 'FAILED'`) != 1 {
		t.Fatal("budget refusal retry duplicated the FAILED job")
	}
}
