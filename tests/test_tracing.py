"""What the tracing wiring must not do.

The trace itself is exercised end to end against a real collector, which a unit
test cannot stand in for. What is here is the three properties that would
otherwise be discovered in production: a credential in a span attribute, a
metric series per job, and a cost paid by everyone who has no collector.
"""

from __future__ import annotations

import re

import pytest

from gpu_broker import metrics as metrics_module
from gpu_broker import tracing
from gpu_broker.errors import BackendError
from gpu_broker.states import JobState


# --- redaction --------------------------------------------------------------

def test_a_command_never_becomes_an_attribute():
    """The submitted command is the payload most likely to carry a token."""
    raw = "python train.py --hf-token=hf_ABCDEFGH12345678 --lr 3e-4"
    out = tracing.sanitise({"command": raw})

    assert "command" not in out, "the command itself must not survive"
    assert out["command.program"] == "python"
    assert out["command.length"] == len(raw)
    assert not any("hf_ABCDEFGH12345678" in str(v) for v in out.values())


@pytest.mark.parametrize("secret", [
    "hf_ABCDEFGH12345678",
    "sk-abcdefgh12345678",
    "AKIAIOSFODNN7EXAMPLE",
    "ghp_abcdefghijklmnop1234",
    "eyJhbGciOi.eyJzdWIiOi.SflKxwRJSM",
    "authorization: Bearer abcdefgh12345678",
])
def test_credential_shapes_are_scrubbed(secret):
    assert secret not in tracing.scrub(f"connecting with {secret} now")
    assert tracing.REDACTED in tracing.scrub(f"connecting with {secret} now")


def test_a_live_credential_is_scrubbed_by_value(monkeypatch):
    """Not every credential is a recognisable shape. The ones this process is
    actually holding are removed by exact match as well."""
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "notashapeanyoneknows123")
    out = tracing.sanitise({"detail": "failed with notashapeanyoneknows123"})
    assert "notashapeanyoneknows123" not in out["detail"]


def test_an_exception_message_is_scrubbed_too(monkeypatch):
    """The path that produced this test: exception messages reach the span
    through a different door than attributes, and the SDK's own recorder does
    not scrub them."""
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "notashapeanyoneknows123")
    out = tracing.sanitise({
        "exception.message": "cannot reach s3: notashapeanyoneknows123 rejected",
    })
    assert "notashapeanyoneknows123" not in out["exception.message"]


def test_an_undeclared_container_is_summarised_not_serialised():
    out = tracing.sanitise({"whatever": {"SUPABASE_KEY": "eyJhbGciOi.eyJzdWIiOi.SflKxwRJSM"}})
    assert out == {"whatever.length": 1}


def test_a_long_attribute_is_truncated():
    out = tracing.sanitise({"detail": "x" * 5_000})
    assert len(out["detail"]) < tracing.MAX_ATTR_CHARS + 64


# --- cardinality ------------------------------------------------------------

def test_identifiers_are_span_attributes_and_never_metric_labels(
    broker, cloud, clock, monkeypatch
):
    """The whole reason both exist.

    `job_id` on a span is the point: it is how you find the one request. The
    same string as a Prometheus label is a new time series per job, forever.
    This asserts the second half - that nothing the tracing work added shows up
    in the scrape body.
    """
    broker.add_user("ana")
    for _ in range(12):
        broker.submit(user_id="ana", command="python train.py", gpu_type="a10g", hours=0.5)
        clock.advance(minutes=1)
    broker.tick()

    body = metrics_module.prometheus(broker.metrics)
    labels = set(re.findall(r'[{,]([a-z_]+)="', body))
    leaked = labels & tracing.FORBIDDEN_METRIC_LABELS
    assert not leaked, f"identifier-shaped Prometheus labels: {sorted(leaked)}"


def test_the_scrape_body_does_not_grow_with_the_number_of_jobs(broker, cloud, clock):
    """The bound that matters is a bound on *series*, not on span count.

    Traces are per request and are sampled and expired by the backend. Metric
    series are not: one new label set is one new series in Prometheus for the
    retention period, and a series per job is how a scrape endpoint becomes the
    thing that falls over.
    """
    broker.add_user("ana")

    def series_count() -> int:
        body = metrics_module.prometheus(broker.metrics)
        return len([line for line in body.splitlines() if line and not line.startswith("#")])

    def run(count: int) -> int:
        for _ in range(count):
            broker.submit(user_id="ana", command="python train.py",
                          gpu_type="a10g", hours=0.01)
        for _ in range(6):
            broker.tick()
            clock.advance(minutes=10)
        return series_count()

    few = run(2)
    many = run(30)
    assert many <= few + 4, (
        f"{few} series after 2 jobs, {many} after 32. The scrape body is growing "
        "with the number of jobs, which is a series per job in Prometheus forever."
    )


# --- the cost of being off --------------------------------------------------

def test_spans_are_a_no_op_with_no_collector_configured(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    tracing.setup("test")
    assert not tracing.enabled()
    with tracing.span("anything", job_id="x") as span:
        assert span.trace_id is None
        span.set_attribute("k", "v")
        span.failed("nope")
    assert tracing.current_traceparent() is None


def test_the_queue_hop_survives_a_broken_telemetry_table(broker, monkeypatch):
    """A job that was admitted stays admitted.

    Deliberate: `record_queue_context` swallows its own database errors. An
    untraceable job is a worse day; an unadmitted one is a bug.
    """
    broker.add_user("ana")
    broker.store.conn.execute("DROP TABLE trace_context")
    result = broker.submit(user_id="ana", command="python train.py",
                           gpu_type="a10g", hours=0.5)
    assert result.accepted
    assert result.job.state is JobState.QUEUED
    assert tracing.queue_context(broker.store.conn, result.job.job_id) is None


# --- what the spans actually carry -------------------------------------------
# Everything above asserts on the input to the wiring. These read the spans back
# out of a real SDK exporter, because a rule is only worth something at the point
# telemetry leaves the process.

@pytest.fixture(scope="session")
def _collector():
    """One exporter for the whole session.

    The SDK installs a global tracer provider once per process and refuses to
    replace it, so a per-test provider would be built, ignored, and its spans would
    go to the first test's exporter.
    """
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    shutdown = tracing.setup("gpu-broker-test", exporter=exporter)
    yield exporter
    tracing.flush()
    shutdown()


@pytest.fixture
def exported(_collector):
    tracing.setup("gpu-broker-test", exporter=_collector)
    _collector.clear()
    yield _collector
    tracing.flush()


def spans_by_name(exporter):
    return {span.name: span for span in exporter.get_finished_spans()}


def test_a_submission_and_the_dispatch_that_acts_on_it_are_one_trace(
    exported, broker, cloud, clock
):
    """The whole point of the queue hop.

    `gpu web` and `gpu run` are separate processes that meet only through a row, so
    without the parked context these are two traces and "where did the request go"
    has no answer. Asserted on the trace ids rather than on the wiring, because
    parking a context that nothing reparents onto would pass any test of the wiring
    alone.
    """
    broker.add_user("ana")
    result = broker.submit(user_id="ana", command="python train.py",
                           gpu_type="a10g", hours=0.5)
    assert result.accepted
    clock.advance(minutes=1)
    broker.tick()

    spans = spans_by_name(exported)
    enqueue = spans["queue.enqueue"]
    dispatch = spans["queue.dispatch"]
    assert dispatch.parent is not None, "the dispatch span is a root: the hop was lost"
    assert dispatch.context.trace_id == enqueue.context.trace_id
    assert dispatch.parent.span_id == enqueue.context.span_id
    assert dispatch.attributes["outcome"] == "dispatched"


def test_a_dispatch_that_could_not_get_a_machine_does_not_record_success(
    exported, broker, cloud, clock, monkeypatch
):
    """The failure this system actually has does not propagate.

    The scheduler catches BackendError and puts the job back in the queue, so a span
    that inferred its outcome from "did this block raise" would record ok for a
    dispatch that never got a machine - a trace that says the system is fine.
    """
    broker.add_user("ana")
    result = broker.submit(user_id="ana", command="python train.py --hf-token=hf_ABCDEFGH12345678",
                           gpu_type="a10g", hours=0.5)
    assert result.accepted

    def refuse(job):
        raise BackendError("no capacity in us-west-2 for hf_ABCDEFGH12345678")

    monkeypatch.setattr(cloud, "launch", refuse)
    clock.advance(minutes=1)
    broker.tick()

    dispatch = spans_by_name(exported)["queue.dispatch"]
    assert dispatch.status.status_code.name == "ERROR"
    assert dispatch.attributes["outcome"] == "launch_failed"
    assert dispatch.attributes["error_type"] == "BackendError"
    # The reason is kept, because "the launch failed" without it is not actionable -
    # but it goes through the scrubber like every other string.
    assert "hf_ABCDEFGH12345678" not in str(dict(dispatch.attributes))
    assert tracing.REDACTED in dispatch.attributes["error_message"]


def test_the_submitted_command_never_reaches_a_span(exported, broker, cloud, clock):
    """The end-to-end version of the redaction rule, through the real submit path."""
    broker.add_user("ana")
    broker.submit(user_id="ana",
                  command="python train.py --hf-token=hf_ABCDEFGH12345678 --lr 3e-4",
                  gpu_type="a10g", hours=0.5)

    enqueue = spans_by_name(exported)["queue.enqueue"]
    attrs = dict(enqueue.attributes)
    assert "command" not in attrs
    assert attrs["command.program"] == "python"
    assert not any("hf_ABCDEFGH12345678" in str(v) for v in attrs.values())


def test_no_span_is_built_with_no_collector_configured(_collector, monkeypatch):
    """Not "no trace id leaks" - no span is BUILT. The deployment with no collector
    is the common one and it must pay for a dict lookup, not for an SDK span it then
    drops."""
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    tracing.setup("gpu-broker-test")
    _collector.clear()
    assert not tracing.enabled()
    with tracing.span("queue.dispatch", job_id="x") as span:
        span.failed("nope")
    assert _collector.get_finished_spans() == (), "a span was built with no collector"
