package agent

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// KubeRuntime runs each attempt as a batch/v1 Job in a dedicated namespace.
// This is the production isolation boundary (ADR 0007): the user's image
// runs as non-root with no capabilities, no service-account token, a
// read-only root filesystem and a hard deadline, pinned to this agent's node.
type KubeRuntime struct {
	Client    kubernetes.Interface
	Namespace string
	NodeName  string
	WorkerID  domain.WorkerID
	// GPUResource is the extended resource to request ("nvidia.com/gpu").
	// Empty in the Kind profile, where GPUs are declared, not real.
	GPUResource string
	Tolerations []corev1.Toleration
	CPU, Memory string // per-attempt limits, e.g. "1", "2Gi"

	mu       sync.Mutex
	logsSeen map[domain.AttemptID]int
}

const (
	labelAttempt = "gpub.dev/attempt"
	labelWorker  = "gpub.dev/worker"
	labelJob     = "gpub.dev/job"
)

func (k *KubeRuntime) Name() string { return "kubernetes" }

var dnsUnsafe = regexp.MustCompile(`[^a-z0-9-]`)

// jobName is deterministic in the attempt id, which is what makes Start
// idempotent: a second create after a restart hits AlreadyExists.
func jobName(id domain.AttemptID) string {
	n := "gpub-" + dnsUnsafe.ReplaceAllString(strings.ToLower(string(id)), "-")
	if len(n) > 63 {
		n = n[:63]
	}
	return strings.TrimRight(n, "-")
}

func labelValue(s string) string {
	v := dnsUnsafe.ReplaceAllString(strings.ToLower(s), "-")
	if len(v) > 63 {
		v = v[:63]
	}
	return strings.Trim(v, "-")
}

// Manifest builds the Job. Exported so a test can assert every security
// field without a cluster.
func (k *KubeRuntime) Manifest(s Spec) *batchv1.Job {
	f, t := false, true
	uid := int64(65532)
	zero := int32(0)
	deadline := int64(s.MaxRuntime.Seconds())
	limits := corev1.ResourceList{}
	if k.CPU != "" {
		limits[corev1.ResourceCPU] = resource.MustParse(k.CPU)
	}
	if k.Memory != "" {
		limits[corev1.ResourceMemory] = resource.MustParse(k.Memory)
	}
	if k.GPUResource != "" {
		limits[corev1.ResourceName(k.GPUResource)] = *resource.NewQuantity(int64(s.GPUs), resource.DecimalSI)
	}
	labels := map[string]string{labelAttempt: labelValue(string(s.AttemptID)), labelWorker: labelValue(string(k.WorkerID)),
		labelJob: labelValue(string(s.JobID)), "app.kubernetes.io/managed-by": "gpubroker-agent"}
	var cmd []string
	var args []string
	if len(s.Command) > 0 {
		cmd, args = s.Command[:1], s.Command[1:]
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName(s.AttemptID), Namespace: k.Namespace, Labels: labels,
			Annotations: map[string]string{"gpub.dev/attempt-id": string(s.AttemptID)}},
		Spec: batchv1.JobSpec{
			// Retries belong to the control plane (a new attempt each time),
			// never to the Job controller, or one attempt could run twice.
			BackoffLimit: &zero,
			// A pod disrupted by a drain, preemption or eviction fails the Job
			// with reason PodFailurePolicy and a message naming
			// DisruptionTarget. That is how Poll tells "the node went away"
			// (LOST, retried as a new attempt) from "the user's code failed"
			// (FAILED, not retried).
			PodFailurePolicy: &batchv1.PodFailurePolicy{Rules: []batchv1.PodFailurePolicyRule{{
				Action:          batchv1.PodFailurePolicyActionFailJob,
				OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue}},
			}}},
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: ptr32(3600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					NodeName:                     k.NodeName,
					AutomountServiceAccountToken: &f,
					EnableServiceLinks:           &f,
					Tolerations:                  k.Tolerations,
					SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &t, RunAsUser: &uid, RunAsGroup: &uid,
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					Containers: []corev1.Container{{
						Name: "job", Image: s.Image, Command: cmd, Args: args,
						Resources: corev1.ResourceRequirements{Limits: limits, Requests: limits},
						SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &f, ReadOnlyRootFilesystem: &t,
							Privileged: &f, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
						VolumeMounts: []corev1.VolumeMount{{Name: "scratch", MountPath: "/tmp"}},
					}},
					Volumes: []corev1.Volume{{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
				},
			},
		},
	}
}

func ptr32(v int32) *int32 { return &v }

func (k *KubeRuntime) Start(ctx context.Context, s Spec) error {
	_, err := k.Client.BatchV1().Jobs(k.Namespace).Create(ctx, k.Manifest(s), metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (k *KubeRuntime) Poll(ctx context.Context, id domain.AttemptID) (Status, error) {
	job, err := k.Client.BatchV1().Jobs(k.Namespace).Get(ctx, jobName(id), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// Deleted under us (node drain evicted and GC'd it, or an operator
		// removed it): the work is gone, so the attempt is LOST and the
		// control plane decides whether to retry.
		return Status{Done: true, Outcome: domain.OutcomeLost, Reason: "kubernetes job no longer exists"}, nil
	}
	if err != nil {
		return Status{}, err
	}
	st := Status{Logs: k.newLogs(ctx, id)}
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete, batchv1.JobSuccessCriteriaMet:
			code := 0
			st.Done, st.Outcome, st.ExitCode, st.Reason = true, domain.OutcomeSucceeded, &code, "kubernetes job complete"
		case batchv1.JobFailed, batchv1.JobFailureTarget:
			st.Done = true
			st.ExitCode = k.exitCode(ctx, id)
			switch c.Reason {
			case "DeadlineExceeded":
				st.Outcome, st.Reason = domain.OutcomeTimedOut, "activeDeadlineSeconds exceeded"
			case "PodFailurePolicy", "BackoffLimitExceeded":
				st.Outcome, st.Reason = domain.OutcomeFailed, c.Message
				if strings.Contains(c.Message, string(corev1.DisruptionTarget)) || k.evicted(ctx, id) {
					st.Outcome, st.Reason = domain.OutcomeLost, "pod disrupted (drain, eviction or preemption): "+c.Message
				}
			default:
				st.Outcome, st.Reason = domain.OutcomeFailed, c.Reason+": "+c.Message
			}
		}
		if st.Done {
			return st, nil
		}
	}
	// No pods and the job is not finished: if the pod was deleted (node
	// drain) the Job controller would recreate it only with backoff > 0,
	// which we never set, so this is terminal.
	if job.Status.Failed > 0 && job.Status.Active == 0 {
		st.Done, st.ExitCode = true, k.exitCode(ctx, id)
		st.Outcome, st.Reason = domain.OutcomeFailed, "pod failed"
		if k.evicted(ctx, id) || st.ExitCode == nil {
			st.Outcome, st.Reason = domain.OutcomeLost, "pod evicted or deleted"
		}
	}
	return st, nil
}

func (k *KubeRuntime) pods(ctx context.Context, id domain.AttemptID) []corev1.Pod {
	list, err := k.Client.CoreV1().Pods(k.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labelAttempt + "=" + labelValue(string(id))})
	if err != nil {
		return nil
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].CreationTimestamp.Before(&list.Items[j].CreationTimestamp) })
	return list.Items
}

func (k *KubeRuntime) exitCode(ctx context.Context, id domain.AttemptID) *int {
	for _, p := range k.pods(ctx, id) {
		for _, cs := range p.Status.ContainerStatuses {
			if cs.State.Terminated != nil {
				c := int(cs.State.Terminated.ExitCode)
				return &c
			}
		}
	}
	return nil
}

func (k *KubeRuntime) evicted(ctx context.Context, id domain.AttemptID) bool {
	for _, p := range k.pods(ctx, id) {
		if p.Status.Reason == "Evicted" || p.DeletionTimestamp != nil {
			return true
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.DisruptionTarget && c.Status == corev1.ConditionTrue {
				return true
			}
		}
	}
	return false
}

// newLogs returns log lines not yet reported, counted per attempt in memory.
// After an agent restart the count starts again, so lines already reported
// are reported a second time under new sequence numbers: logs are
// at-least-once across agent restarts. Terminal status is not affected; it
// is deduplicated by the attempt's single outcome.
func (k *KubeRuntime) newLogs(ctx context.Context, id domain.AttemptID) []string {
	ps := k.pods(ctx, id)
	if len(ps) == 0 {
		return nil
	}
	limit := int64(64 << 10)
	rc, err := k.Client.CoreV1().Pods(k.Namespace).GetLogs(ps[len(ps)-1].Name, &corev1.PodLogOptions{LimitBytes: &limit}).Stream(ctx)
	if err != nil {
		return nil
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.logsSeen == nil {
		k.logsSeen = map[domain.AttemptID]int{}
	}
	seen := k.logsSeen[id]
	if seen >= len(lines) {
		return nil
	}
	k.logsSeen[id] = len(lines)
	return lines[seen:]
}

func (k *KubeRuntime) Stop(ctx context.Context, id domain.AttemptID, _ domain.AttemptOutcome) error {
	bg := metav1.DeletePropagationBackground
	err := k.Client.BatchV1().Jobs(k.Namespace).Delete(ctx, jobName(id), metav1.DeleteOptions{PropagationPolicy: &bg})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (k *KubeRuntime) List(ctx context.Context) ([]domain.AttemptID, error) {
	list, err := k.Client.BatchV1().Jobs(k.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labelWorker + "=" + labelValue(string(k.WorkerID))})
	if err != nil {
		return nil, err
	}
	var out []domain.AttemptID
	for _, j := range list.Items {
		if id := j.Annotations["gpub.dev/attempt-id"]; id != "" {
			out = append(out, domain.AttemptID(id))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// Draining is true while this agent's node is cordoned (kubectl drain sets
// unschedulable first). The agent then reports DRAINING so the scheduler
// stops placing here before the evictions start.
func (k *KubeRuntime) Draining(ctx context.Context) bool {
	if k.NodeName == "" {
		return false
	}
	n, err := k.Client.CoreV1().Nodes().Get(ctx, k.NodeName, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return n.Spec.Unschedulable
}

func (k *KubeRuntime) String() string {
	return fmt.Sprintf("kubernetes(ns=%s node=%s)", k.Namespace, k.NodeName)
}
