package agent

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

func setCondition(t *testing.T, k *KubeRuntime, id domain.AttemptID, typ batchv1.JobConditionType, reason, msg string) {
	t.Helper()
	ctx := context.Background()
	j, err := k.Client.BatchV1().Jobs(k.Namespace).Get(ctx, jobName(id), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	j.Status.Conditions = []batchv1.JobCondition{{Type: typ, Status: corev1.ConditionTrue, Reason: reason, Message: msg}}
	if _, err := k.Client.BatchV1().Jobs(k.Namespace).UpdateStatus(ctx, j, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestKubeRuntimeMapsJobConditionsToOutcomes(t *testing.T) {
	ctx := context.Background()
	k := &KubeRuntime{Client: fake.NewClientset(), Namespace: "gpub-jobs", NodeName: "n1", WorkerID: "wrk_1"}
	cases := []struct {
		id      domain.AttemptID
		typ     batchv1.JobConditionType
		reason  string
		msg     string
		outcome domain.AttemptOutcome
	}{
		{"att_ok", batchv1.JobComplete, "", "", domain.OutcomeSucceeded},
		{"att_fail", batchv1.JobFailed, "BackoffLimitExceeded", "Job has reached the specified backoff limit", domain.OutcomeFailed},
		{"att_deadline", batchv1.JobFailed, "DeadlineExceeded", "Job was active longer than specified deadline", domain.OutcomeTimedOut},
		{"att_drain", batchv1.JobFailed, "PodFailurePolicy", "Container job for pod gpub-jobs/x failed with DisruptionTarget condition", domain.OutcomeLost},
	}
	for _, c := range cases {
		s := Spec{AttemptID: c.id, JobID: "job_1", Image: "busybox:1.36", Command: []string{"true"}, GPUs: 1, MaxRuntime: time.Minute}
		if err := k.Start(ctx, s); err != nil {
			t.Fatal(err)
		}
		if err := k.Start(ctx, s); err != nil { // adopt, not duplicate
			t.Fatalf("second start: %v", err)
		}
		if st, _ := k.Poll(ctx, c.id); st.Done {
			t.Fatalf("%s done before any condition", c.id)
		}
		setCondition(t, k, c.id, c.typ, c.reason, c.msg)
		st, err := k.Poll(ctx, c.id)
		if err != nil || !st.Done || st.Outcome != c.outcome {
			t.Fatalf("%s: %+v %v, want %s", c.id, st, err, c.outcome)
		}
	}
	ids, err := k.List(ctx)
	if err != nil || len(ids) != len(cases) {
		t.Fatalf("list %v %v", ids, err)
	}
	if err := k.Stop(ctx, "att_ok", domain.OutcomeCancelled); err != nil {
		t.Fatal(err)
	}
	if st, _ := k.Poll(ctx, "att_ok"); !st.Done || st.Outcome != domain.OutcomeLost {
		t.Fatalf("deleted job: %+v", st)
	}
	other := &KubeRuntime{Client: k.Client, Namespace: "gpub-jobs", WorkerID: "wrk_2"}
	if ids, _ := other.List(ctx); len(ids) != 0 {
		t.Fatalf("another worker's agent adopted %v", ids)
	}
}
