package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/carbon"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/metrics"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/sim"
)

// defaultDataset resolves from the controlplane directory or, in the image, /.
const defaultDataset = "data/carbon/gb-2026-08-03_2026-09-28.json"

type simReport struct {
	EvidenceClass string       `json:"evidence_class"`
	Notice        string       `json:"notice"`
	Dataset       string       `json:"carbon_dataset"`
	DatasetSHA    string       `json:"carbon_dataset_sha256"`
	Seed          uint64       `json:"seed"`
	Results       []sim.Result `json:"results"`
	DecideLatency []latency    `json:"decide_latency"`
}

type latency struct {
	Workload string  `json:"workload"`
	Policy   string  `json:"policy"`
	Calls    int     `json:"calls"`
	P50us    float64 `json:"p50_us"`
	P99us    float64 `json:"p99_us"`
}

func pick(all []string, choice string) []string {
	if choice == "all" {
		return all
	}
	return strings.Split(choice, ",")
}

func runSim(args []string) error {
	fs := flag.NewFlagSet("sim", flag.ExitOnError)
	workload := fs.String("workload", "all", "workload name(s), comma-separated, or all: "+strings.Join(sim.WorkloadNames, ", "))
	pol := fs.String("policy", "all", "policy name(s) or all: "+strings.Join(policy.BuiltinNames(), ", "))
	seed := fs.Uint64("seed", 42, "random seed; same seed, same results")
	dataset := fs.String("dataset", defaultDataset, "carbon dataset")
	out := fs.String("out", "", "write the JSON report here")
	_ = fs.Parse(args)
	d, err := carbon.Load(*dataset)
	if err != nil {
		return err
	}
	rep := simReport{EvidenceClass: "simulator", Dataset: d.Manifest.Dataset, DatasetSHA: d.Manifest.SHA256, Seed: *seed,
		Notice: "Simulator output over a modelled fleet and synthetic arrivals with real GB carbon data. Not adoption, cost or capacity evidence."}
	for _, w := range pick(sim.WorkloadNames, *workload) {
		sc, err := sim.Workload(w, d.Regional, d.Start, *seed)
		if err != nil {
			return err
		}
		for _, pn := range pick(policy.BuiltinNames(), *pol) {
			p, err := policy.Lookup(pn)
			if err != nil {
				return err
			}
			r := sim.Run(sc, p)
			rep.Results = append(rep.Results, r)
			rep.DecideLatency = append(rep.DecideLatency, latency{Workload: w, Policy: pn, Calls: len(r.DecideNanos),
				P50us: pctNanos(r.DecideNanos, .5) / 1e3, P99us: pctNanos(r.DecideNanos, .99) / 1e3})
		}
	}
	printResults(rep.Results)
	if *out != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		return os.WriteFile(*out, append(b, '\n'), 0o644)
	}
	return nil
}

func pctNanos(ns []int64, q float64) float64 {
	if len(ns) == 0 {
		return 0
	}
	s := append([]int64(nil), ns...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(q*float64(len(s))+0.5) - 1
	return float64(s[max(0, min(i, len(s)-1))])
}

func printResults(rs []sim.Result) {
	fmt.Println("SIMULATOR OUTPUT (evidence_class=simulator): modelled fleet, synthetic arrivals, real GB carbon data.")
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "workload\tpolicy\tdone\tunfinished\tp50 wait h\tp99 wait h\tmax wait h\tdeadline miss\tutil\tjain\tcost $\tCO2 kg\tdelays\tpreempt\tfallback\t")
	for _, r := range rs {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%.2f\t%.2f\t%.2f\t%d/%d\t%.3f\t%.3f\t%.0f\t%.1f\t%d\t%d\t%d\t\n", r.Workload, r.Policy, r.Completed,
			r.Unfinished, r.WaitHoursP50, r.WaitHoursP99, r.WaitHoursMax, r.DeadlineMisses, r.DeadlineJobs, r.Utilization, r.JainFairness,
			r.CostUSD, r.CarbonKg, r.Delays, r.Preemptions, r.FallbackDecisions)
	}
	tw.Flush()
}

func runReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	path := fs.String("trace", "", "trace file (gpu-broker.schedule-trace/v1 or gpu-broker.controlplane-trace/v1)")
	capacity := fs.Int("capacity", 1, "declared GPUs per GPU type (traces do not record capacity)")
	pol := fs.String("policy", "fifo,fair-share,priority,deadline-first", "policies to compare")
	_ = fs.Parse(args)
	sc, info, err := sim.LoadTrace(*path, *capacity)
	if err != nil {
		return err
	}
	var rs []sim.Result
	for _, pn := range pick(policy.BuiltinNames(), *pol) {
		p, err := policy.Lookup(pn)
		if err != nil {
			return err
		}
		rs = append(rs, sim.Run(sc, p))
	}
	b, _ := json.MarshalIndent(info, "", "  ")
	fmt.Println(string(b))
	printResults(rs)
	return nil
}

type forecastReport struct {
	Dataset     string                     `json:"dataset"`
	DatasetSHA  string                     `json:"dataset_sha256"`
	Series      string                     `json:"series"`
	TestWindow  [2]time.Time               `json:"test_window"`
	Horizon     string                     `json:"horizon"`
	AnchorEvery string                     `json:"anchor_every"`
	Models      map[string]ecoshift.Errors `json:"models"`
	Verdict     string                     `json:"verdict"`
}

// runForecastEval scores the seasonal model against baselines on the held-
// out final week of the observed national series. ESO's own day-ahead
// forecast is scored on the same target intervals.
func runForecastEval(args []string) error {
	fs := flag.NewFlagSet("forecast-eval", flag.ExitOnError)
	dataset := fs.String("dataset", defaultDataset, "carbon dataset")
	out := fs.String("out", "", "write JSON here")
	_ = fs.Parse(args)
	d, err := carbon.Load(*dataset)
	if err != nil {
		return err
	}
	s := ecoshift.NewSeries(d.NationalActual)
	end := d.End()
	from, to := end.Add(-7*24*time.Hour), end.Add(-24*time.Hour)
	bt := s.Backtest(from, to, time.Hour, 24*time.Hour)
	eso := ecoshift.NewSeries(d.ESOForecast)
	var pred, act []float64
	for anchor := from; anchor.Before(to); anchor = anchor.Add(time.Hour) {
		for t := anchor.Add(ecoshift.Step); !t.After(anchor.Add(24 * time.Hour)); t = t.Add(ecoshift.Step) {
			a, okA := s[t]
			f, okF := eso[t]
			_, okS := s.Seasonal(t, anchor)
			if okA && okF && okS {
				pred, act = append(pred, f), append(act, a)
			}
		}
	}
	bt["eso-published-forecast"] = ecoshift.ErrorsOf(pred, act)
	rep := forecastReport{Dataset: d.Manifest.Dataset, DatasetSHA: d.Manifest.SHA256, Series: "GB national actual (observed)",
		TestWindow: [2]time.Time{from, end}, Horizon: "24h", AnchorEvery: "1h", Models: bt}
	seas, pers := bt[ecoshift.SeasonalModel], bt["persistence"]
	if seas.MAE < pers.MAE {
		rep.Verdict = fmt.Sprintf("%s beats persistence (MAE %.1f vs %.1f g/kWh)", ecoshift.SeasonalModel, seas.MAE, pers.MAE)
	} else {
		rep.Verdict = fmt.Sprintf("%s does NOT beat persistence (MAE %.1f vs %.1f g/kWh); the policy disables delay where this holds",
			ecoshift.SeasonalModel, seas.MAE, pers.MAE)
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		return os.WriteFile(*out, append(b, '\n'), 0o644)
	}
	return nil
}

func runAdmin(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("admin needs a subcommand: create-tenant | create-pool | load-carbon | leader")
	}
	fs := flag.NewFlagSet("admin "+args[0], flag.ExitOnError)
	var c common
	c.bind(fs)
	c.metricsAddr = ""
	name := fs.String("name", "", "tenant or pool name")
	evidence := fs.String("evidence", "real", "tenant evidence class: real | pilot | seeded")
	handle := fs.String("handle", "", "first user's handle")
	budget := fs.Float64("budget-usd", 100, "default project budget")
	id := fs.String("id", "", "pool id")
	region := fs.String("region", "", "pool region (must match a carbon region to get carbon data)")
	kind := fs.String("kind", "kubernetes", "pool kind: kubernetes | simulated")
	price := fs.Float64("price-usd", 0, "pool price per GPU-hour")
	interruptible := fs.Bool("interruptible", false, "pool capacity can be reclaimed (spot)")
	pue := fs.Float64("pue", 1.0, "pool power usage effectiveness")
	dataset := fs.String("dataset", defaultDataset, "carbon dataset (load-carbon)")
	mapping := fs.String("map", "", "dataset-region=pool-region pairs, comma-separated (load-carbon)")
	replay := fs.Bool("replay-now", false, "shift the dataset to end now and label it replay:<dataset> (demo only)")
	_ = fs.Parse(args[1:])
	if os.Getenv("GPUB_DISPATCH_KEY") == "" && os.Getenv("GPUB_DISPATCH_KEY_FILE") == "" {
		// Admin commands never sign dispatches; a throwaway key satisfies the
		// constructor without requiring the real secret on an operator laptop.
		os.Setenv("GPUB_DISPATCH_KEY", strings.Repeat("a", 32))
	}
	svc, store, err := service(ctx, c, metrics.New(), nil)
	if err != nil {
		return err
	}
	defer store.Close()
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	switch args[0] {
	case "create-tenant":
		tc, err := svc.CreateTenant(ctx, application.TenantSetup{Name: *name, EvidenceClass: domain.EvidenceClass(*evidence), Handle: *handle, BudgetUSD: *budget})
		if err != nil {
			return err
		}
		return enc.Encode(tc)
	case "create-pool":
		tok, err := svc.UpsertPool(ctx, domain.Pool{ID: domain.PoolID(*id), Name: *name, Kind: domain.PoolKind(*kind), Region: *region,
			PriceMicroUSDPerGPUHour: domain.MicroUSD(*price * 1e6), Interruptible: *interruptible, PUE: *pue})
		if err != nil {
			return err
		}
		return enc.Encode(map[string]string{"pool_id": *id, "bootstrap_token": tok})
	case "load-carbon":
		d, err := carbon.Load(*dataset)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		var at *time.Time
		if *replay {
			at = &now
		}
		loaded := map[string]int{}
		for _, pair := range strings.Split(*mapping, ",") {
			from, to, ok := strings.Cut(pair, "=")
			if !ok || d.Regional[from] == nil {
				return fmt.Errorf("bad --map entry %q (dataset regions: %v)", pair, d.RegionNames())
			}
			snaps := d.Snapshots(from, to, at, now)
			if err := store.InTx(ctx, func(tx application.Tx) error { return tx.UpsertCarbon(ctx, snaps) }); err != nil {
				return err
			}
			loaded[to] = len(snaps)
		}
		return enc.Encode(map[string]any{"dataset": d.Manifest.Dataset, "replay": *replay, "loaded": loaded})
	case "leader":
		epoch, holder, err := store.CurrentEpoch(ctx)
		if err != nil {
			return err
		}
		return enc.Encode(map[string]any{"epoch": epoch, "holder": holder})
	}
	return fmt.Errorf("unknown admin subcommand %q", args[0])
}
