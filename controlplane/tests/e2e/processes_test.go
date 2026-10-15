package e2e_test

import (
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
)

type child struct {
	cmd  *exec.Cmd
	done chan struct{}
}

func launch(t *testing.T, binary string, env []string, args ...string) *child {
	t.Helper()
	log, err := os.CreateTemp(t.TempDir(), "process-*.log")
	if err != nil {
		t.Fatal(err)
	}
	c := &child{cmd: exec.Command(binary, args...), done: make(chan struct{})}
	c.cmd.Env = append(os.Environ(), env...)
	c.cmd.Stdout, c.cmd.Stderr = log, log
	if err := c.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = c.cmd.Wait()
		close(c.done)
	}()
	t.Cleanup(func() {
		select {
		case <-c.done:
		default:
			_ = c.cmd.Process.Kill()
			<-c.done
		}
		_ = log.Close()
		if t.Failed() {
			data, _ := os.ReadFile(log.Name())
			t.Logf("%s: %s", strings.Join(args, " "), data)
		}
	})
	return c
}

func (c *child) kill(t *testing.T) {
	t.Helper()
	if err := c.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		t.Fatal("process did not exit after SIGKILL")
	}
}

func eventually(t *testing.T, label string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}

func processEnv(e *testkit.Env) []string {
	return []string{"GPUB_DATABASE_URL=" + e.URL, "GPUB_DISPATCH_KEY=" + string(testkit.DispatchKey), "GOMAXPROCS=2"}
}

// Opt in because this builds and kills real processes. PostgreSQL is required;
// the worker runtime remains the declared filesystem-backed simulator.
func TestLocalProcessRecovery(t *testing.T) {
	if os.Getenv("GPUB_PROCESS_TESTS") != "1" {
		t.Skip("set GPUB_PROCESS_TESTS=1 to run real process recovery controls")
	}
	_ = testkit.DatabaseURL(t)
	binary := filepath.Join(t.TempDir(), "gpubroker")
	build := exec.Command("go", "build", "-race", "-p", "2", "-o", binary, "../../cmd/gpubroker")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, output)
	}
	t.Run("scheduler-sigkill", func(t *testing.T) {
		e := testkit.New(t)
		e.Svc.Clock = application.SystemClock{}
		user, _ := e.Tenant("process-tenant", 100)
		worker := e.Worker(e.Pool("pool-a", "london", 1, false), "node-1", 1)
		job := e.Submit(user, 1)
		env := processEnv(e)
		a := launch(t, binary, env, "scheduler", "--holder=process-a", "--interval=50ms", "--metrics-addr=")
		b := launch(t, binary, env, "scheduler", "--holder=process-b", "--interval=50ms", "--metrics-addr=")
		eventually(t, "reservation", func() bool { return e.Job(user, job.ID).State == domain.JobReserved })
		dispatches, err := e.Svc.ClaimDispatches(e.Ctx, worker, 1)
		e.Must(err)
		if len(dispatches) != 1 {
			t.Fatalf("dispatches: %+v", dispatches)
		}
		_, err = e.Svc.Ack(e.Ctx, worker, dispatches[0])
		e.Must(err)
		epoch, holder, err := e.Store.CurrentEpoch(e.Ctx)
		e.Must(err)
		if holder == "process-a" {
			a.kill(t)
		} else if holder == "process-b" {
			b.kill(t)
		} else {
			t.Fatalf("unexpected leader %q", holder)
		}
		eventually(t, "standby leadership", func() bool {
			next, name, err := e.Store.CurrentEpoch(e.Ctx)
			return err == nil && next > epoch && name != holder
		})
		code := 0
		for range 2 {
			_, err = e.Svc.ReportEvents(e.Ctx, worker, dispatches[0].AttemptID, []application.WorkerEvent{{Seq: 1, Kind: "exit", Outcome: domain.OutcomeSucceeded, ExitCode: &code}})
			e.Must(err)
		}
		if e.Job(user, job.ID).State != domain.JobSucceeded || e.QueryInt(`SELECT count(*) FROM job_attempts`) != 1 || e.QueryInt(`SELECT sum(gpus_reserved) FROM workers`) != 0 {
			t.Fatal("scheduler SIGKILL lost or duplicated the in-flight attempt")
		}
		next := e.Submit(user, 1)
		eventually(t, "standby placement", func() bool { return e.Job(user, next.ID).State == domain.JobReserved })
	})
	t.Run("agent-and-api-sigkill", func(t *testing.T) {
		e := testkit.New(t)
		e.Svc.Clock = application.SystemClock{}
		user, _ := e.Tenant("process-tenant", 100)
		boot, err := e.Svc.UpsertPool(e.Ctx, domain.Pool{ID: "pool-a", Name: "pool-a", Kind: domain.PoolSimulated, Region: "london", PriceMicroUSDPerGPUHour: 1_000_000})
		e.Must(err)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		e.Must(err)
		addr := listener.Addr().String()
		e.Must(listener.Close())
		url := "http://" + addr
		env := append(processEnv(e), "GPUB_BOOTSTRAP_TOKEN="+boot)
		objects, state := t.TempDir(), t.TempDir()
		startAPI := func() *child {
			p := launch(t, binary, env, "api", "--addr="+addr, "--object-dir="+objects, "--metrics-addr=")
			eventually(t, "API readiness", func() bool {
				client := http.Client{Timeout: 200 * time.Millisecond}
				response, err := client.Get(url + "/readyz")
				if err != nil {
					return false
				}
				_ = response.Body.Close()
				return response.StatusCode == http.StatusOK
			})
			return p
		}
		startAgent := func(name, directory string) *child {
			return launch(t, binary, env, "worker-agent", "--api-url="+url, "--name="+name, "--state-dir="+directory, "--heartbeat=100ms", "--runtime=sim")
		}
		api := startAPI()
		agent := startAgent("node-1", state)
		eventually(t, "worker registration", func() bool { return e.QueryInt(`SELECT count(*) FROM workers WHERE state = 'READY'`) == 1 })
		submit := func(duration string) domain.JobID {
			e.Svc.Clock = application.SystemClock{}
			job := e.Submit(user, 1, func(request *application.SubmitRequest) {
				request.Runtime, request.Command = "sim", []string{"sim", "duration=" + duration, "artifact_bytes=16"}
			})
			e.Tick()
			eventually(t, "running attempt", func() bool { return e.Job(user, job.ID).State == domain.JobRunning })
			return job.ID
		}
		job := submit("3s")
		agent.kill(t)
		agent = startAgent("node-1", state)
		eventually(t, "same-attempt recovery", func() bool { return e.Job(user, job).State == domain.JobSucceeded })
		if e.QueryInt(`SELECT count(*) FROM job_attempts WHERE job_id = $1`, job) != 1 || e.QueryInt(`SELECT count(*) FROM workers`) != 1 {
			t.Fatal("agent restart duplicated the attempt or worker")
		}
		job = submit("2s")
		api.kill(t)
		time.Sleep(3 * time.Second)
		api = startAPI()
		eventually(t, "API outage recovery", func() bool { return e.Job(user, job).State == domain.JobSucceeded })
		if e.QueryInt(`SELECT count(*) FROM attempt_events e JOIN job_attempts a ON a.id = e.attempt_id WHERE a.job_id = $1 AND e.kind = 'exit'`, job) != 1 {
			t.Fatal("API restart lost or duplicated the terminal report")
		}
		job = submit("2s")
		agent.kill(t)
		e.Svc.Clock = application.SystemClock{}
		// Explicit timer fault injection avoids pretending this observes a
		// natural 45-second lease or 90-second heartbeat timeout.
		e.Exec(`UPDATE leases SET expires_at = $1`, time.Now().Add(-time.Second))
		e.Exec(`UPDATE workers SET last_heartbeat = $1`, time.Now().Add(-2*time.Minute))
		repaired, err := e.Svc.Reconcile(e.Ctx)
		e.Must(err)
		if repaired.ExpiredLeases != 1 || repaired.WorkersOffline != 1 {
			t.Fatalf("killed worker reclaim: %+v", repaired)
		}
		startAgent("node-2", t.TempDir())
		eventually(t, "replacement worker", func() bool { return e.QueryInt(`SELECT count(*) FROM workers WHERE state = 'READY'`) == 1 })
		e.Tick()
		eventually(t, "retry completion", func() bool { return e.Job(user, job).State == domain.JobSucceeded })
		if e.QueryInt(`SELECT count(*) FROM job_attempts WHERE job_id = $1`, job) != 2 || e.QueryInt(`SELECT count(*) FROM job_attempts WHERE job_id = $1 AND outcome = 'LOST'`, job) != 1 || e.QueryInt(`SELECT sum(gpus_reserved) FROM workers`) != 0 {
			t.Fatal("worker loss did not settle and recover exactly once")
		}
	})
}
