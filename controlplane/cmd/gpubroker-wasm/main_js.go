//go:build js && wasm

// Package main exposes the existing Go scheduler simulator to a browser worker.
// The worker supplies the checked-in carbon fixture and a bounded scenario;
// this entry point never contacts a provider or submits a GPU job.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"syscall/js"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/carbon"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/sim"
)

const maxRequestBytes = 128 << 10

type request struct {
	CarbonFixture json.RawMessage `json:"carbon_fixture"`
	Workload      string          `json:"workload"`
	Policy        string          `json:"policy"`
	Seed          uint64          `json:"seed"`
	MaxJobs       int             `json:"max_jobs"`
}

type response struct {
	Error         string      `json:"error,omitempty"`
	EvidenceClass string      `json:"evidence_class,omitempty"`
	Workload      string      `json:"workload,omitempty"`
	Policy        string      `json:"policy,omitempty"`
	PolicyVersion string      `json:"policy_version,omitempty"`
	Jobs          int         `json:"jobs,omitempty"`
	HorizonHours  int         `json:"horizon_hours,omitempty"`
	Result        *sim.Result `json:"result,omitempty"`
}

func run(raw string) string {
	if len(raw) > maxRequestBytes {
		return encode(response{Error: "request exceeds 128 KiB"})
	}
	var req request
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return encode(response{Error: "invalid request: " + err.Error()})
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return encode(response{Error: "request must contain exactly one JSON value"})
	}
	if len(req.CarbonFixture) == 0 || req.Workload == "" || req.Policy == "" {
		return encode(response{Error: "carbon fixture, workload, and policy are required"})
	}
	dataset, err := carbon.Parse(req.CarbonFixture)
	if err != nil {
		return encode(response{Error: "carbon fixture rejected: " + err.Error()})
	}
	workload, err := sim.DemoWorkload(req.Workload, dataset.Regional, dataset.Start, req.Seed, req.MaxJobs)
	if err != nil {
		return encode(response{Error: err.Error()})
	}
	selected, err := policy.Lookup(req.Policy)
	if err != nil {
		return encode(response{Error: err.Error()})
	}
	result := sim.Run(workload, selected)
	return encode(response{EvidenceClass: result.EvidenceClass, Workload: result.Workload,
		Policy: selected.Name, PolicyVersion: selected.Version, Jobs: len(workload.Jobs),
		HorizonHours: int(workload.Horizon.Hours()), Result: &result})
}

func encode(value response) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf(`{"error":%q}`, "could not encode simulator result")
	}
	return string(raw)
}

func main() {
	callback := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return encode(response{Error: "expected one JSON string argument"})
		}
		return run(args[0].String())
	})
	js.Global().Set("gpuBrokerRun", callback)
	select {}
}
