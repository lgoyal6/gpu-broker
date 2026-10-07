package application

import (
	"crypto/rand"
	"encoding/base32"
	"strings"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

// Config holds the timing and backpressure knobs. Every field has a reason
// to exist in a test or a runbook; see docs/runbooks.
type Config struct {
	LeaseTTL           time.Duration // a worker must renew within this
	ClaimTimeout       time.Duration // RESERVED longer than this: the worker never claimed it
	WorkerOfflineAfter time.Duration
	TimeoutGrace       time.Duration // beyond max runtime before a stop is requested
	DispatchVisibility time.Duration // outbox redelivery delay after an abandoned claim
	MaxQueueScan       int           // jobs loaded per tick: bounds scheduler memory
	MaxQueuedGlobal    int           // submissions refused with queue_full beyond this
	FairShareWindow    time.Duration
	FairShareHalfLife  time.Duration
	DefaultPolicy      string
	CarbonHistory      time.Duration
}

func DefaultConfig() Config {
	return Config{
		LeaseTTL:           45 * time.Second,
		ClaimTimeout:       2 * time.Minute,
		WorkerOfflineAfter: 90 * time.Second,
		TimeoutGrace:       time.Minute,
		DispatchVisibility: 30 * time.Second,
		MaxQueueScan:       2000,
		MaxQueuedGlobal:    20000,
		FairShareWindow:    30 * 24 * time.Hour,
		FairShareHalfLife:  14 * 24 * time.Hour,
		DefaultPolicy:      "balanced",
		CarbonHistory:      8 * 24 * time.Hour,
	}
}

// Observer receives the events the metrics adapter turns into series. The
// application never imports Prometheus.
type Observer interface {
	Transition(from, to domain.JobState, ec domain.EvidenceClass)
	Decision(d policy.Decision)
	TickDuration(d time.Duration, decisions int)
	ReservationConflict(reason string)
	StaleLease(kind string)
	AttemptOutcome(o domain.AttemptOutcome)
	StageLatency(stage string, d time.Duration)
	ArtifactFailure(reason string)
	Repair(kind string, n int)
	Fenced()
}

type NopObserver struct{}

func (NopObserver) Transition(domain.JobState, domain.JobState, domain.EvidenceClass) {}
func (NopObserver) Decision(policy.Decision)                                          {}
func (NopObserver) TickDuration(time.Duration, int)                                   {}
func (NopObserver) ReservationConflict(string)                                        {}
func (NopObserver) StaleLease(string)                                                 {}
func (NopObserver) AttemptOutcome(domain.AttemptOutcome)                              {}
func (NopObserver) StageLatency(string, time.Duration)                                {}
func (NopObserver) ArtifactFailure(string)                                            {}
func (NopObserver) Repair(string, int)                                                {}
func (NopObserver) Fenced()                                                           {}

// Service is constructed once per process with every dependency explicit.
// It holds no mutable state of its own; everything durable is in the store.
type Service struct {
	Store    Store
	Clock    Clock
	IDs      IDGen
	Signer   *LeaseSigner
	Objects  ObjectStore
	Obs      Observer
	Cfg      Config
	Policies map[string]policy.Policy
}

func NewService(store Store, clock Clock, ids IDGen, signer *LeaseSigner, objects ObjectStore, obs Observer, cfg Config) *Service {
	if obs == nil {
		obs = NopObserver{}
	}
	return &Service{Store: store, Clock: clock, IDs: ids, Signer: signer, Objects: objects, Obs: obs, Cfg: cfg,
		Policies: policy.Builtins()}
}

// RandomIDs generates prefixed ids from crypto/rand: 80 bits, base32.
type RandomIDs struct{}

func (RandomIDs) New(prefix string) string {
	var b [10]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand failing is not recoverable
	}
	return prefix + "_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))
}
