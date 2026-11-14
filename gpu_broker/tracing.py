"""Distributed tracing across the two processes and the queue between them.

`gpu web` accepts a submission and writes a queued row. `gpu run`, a different
process that may not have been running at the time, later picks that row up,
asks a backend for a machine, and writes the result back. Those are the three
places this system waits: the API call, the queue, and SQLite. Until now a slow
submission and a slow dispatch were two unrelated piles of stderr with no shared
identifier, and the only way to say which of the three was responsible was to
add prints and run it again.

OpenTelemetry rather than something hand-rolled, for one reason that is not
taste: the hop is a *process boundary*. Correlating across it means agreeing on
a wire format for the parent context, and W3C `traceparent` is that format
already, with an extractor, an injector and a viewer that draws the result. A
hand-rolled span id would have had to reinvent all three, and the third one -
the viewer that shows you which span is fat - is most of the value.

Three rules this module enforces rather than documents:

**Off unless a collector is configured.** No `OTEL_EXPORTER_OTLP_ENDPOINT`
means `setup()` installs nothing, `span()` yields a no-op handle, and the cost
is a dict lookup. A club tool that requires a collector to run is a club tool
nobody runs.

**High cardinality belongs on spans, never on metrics.** A job id as a span
attribute is the point of tracing: it is what lets you find the one request.
The same string as a Prometheus label is a new time series forever. This module
therefore emits no metrics at all, and `FORBIDDEN_METRIC_LABELS` records the
names that must not cross over. `gpu_broker.metrics` stays the only writer of
series.

**Nothing that could be a credential becomes an attribute.** A job command is a
shell line a member typed; `--hf-token=hf_...` in it is not hypothetical. The
command is never an attribute in full, only its first word and its length, and
every string attribute passes the scrubber on the way through.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import re
import sqlite3

# --- what may never become an attribute -------------------------------------

# Names whose values are payloads: a member's shell command, an environment, a
# reason string echoed back from a backend. Recorded as a shape, never a value.
_PAYLOAD_ATTRS = frozenset({
    "command", "cmd", "argv", "script", "env", "environ", "config", "payload",
    "body", "headers", "note", "reason", "stderr", "stdout", "log", "logs",
})

_SECRET_PATTERNS = (
    # Deliberately loose. Over-redacting an attribute costs a reader one
    # lookup; under-redacting one puts a credential in a trace that a viewer
    # retains and anyone with the URL can read.
    re.compile(r"hf_[A-Za-z0-9]{8,}"),                                   # HuggingFace
    re.compile(r"sk-[A-Za-z0-9_-]{8,}"),                                 # OpenAI-shaped
    re.compile(r"AKIA[0-9A-Z]{12,}"),                                    # AWS key id
    re.compile(r"ghp_[A-Za-z0-9]{16,}"),                                 # GitHub PAT
    re.compile(r"eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}"),  # JWT
    re.compile(r"(?i)(api[_-]?key|apikey|authorization|bearer|password|secret|token)"
               r"[\"'\s:=]+[A-Za-z0-9_\-.]{8,}"),
)

_SECRET_ENV_VARS = ("AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
                    "GPU_BROKER_GITHUB_CLIENT_SECRET", "GPU_BROKER_METRICS_TOKEN")

REDACTED = "[redacted]"
MAX_ATTR_CHARS = 256

# Identifier-shaped names. Every one of them is welcome on a span and forbidden
# on a metric, and the difference is the whole reason both exist. Asserted in
# tests/test_tracing.py against `gpu_broker.metrics`.
FORBIDDEN_METRIC_LABELS = frozenset({
    "job_id", "trace_id", "span_id", "traceparent", "user_id", "handle",
    "backend_handle", "command", "request_id",
})


def scrub(text: str) -> str:
    """Remove credential-shaped substrings, and any live credential by value."""
    if not isinstance(text, str):
        return text
    for var in _SECRET_ENV_VARS:
        live = os.environ.get(var)
        # A one-character value would turn every attribute into redaction confetti.
        if live and len(live) >= 8:
            text = text.replace(live, REDACTED)
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(REDACTED, text)
    return text


def sanitise(attrs: dict) -> dict:
    """Apply both rules to one attribute mapping.

    A payload-named field keeps its NAME and loses its value: "a command was
    involved, it was 84 characters and it started with python" is the useful
    half, and the other half is what leaks.
    """
    out: dict[str, object] = {}
    for key, value in attrs.items():
        if value is None:
            continue
        if key.lower() in _PAYLOAD_ATTRS:
            if isinstance(value, str):
                head = value.split(" ", 1)[0].rsplit("/", 1)[-1]
                out[key + ".program"] = scrub(head)[:64]
                out[key + ".length"] = len(value)
            else:
                out[key + ".type"] = type(value).__name__
            continue
        if isinstance(value, (dict, list, tuple)):
            out[key + ".length"] = len(value)
            continue
        if isinstance(value, str):
            value = scrub(value)
            if len(value) > MAX_ATTR_CHARS:
                value = value[:MAX_ATTR_CHARS] + f"...<truncated {len(value)}>"
        out[key] = value
    return out


# --- setup ------------------------------------------------------------------

_provider = None
_enabled = False


def enabled() -> bool:
    return _enabled


def setup(service_name: str, *, endpoint: str | None = None, exporter=None):
    """Install a tracer provider if a collector is configured, else nothing.

    Returns a shutdown callable in both cases, so callers have no branch.

    `exporter` exists so a test can read back the spans this module actually emits.
    The rules above are only worth anything at the point telemetry LEAVES the
    process, and asserting on `sanitise` instead asserts on the input to the wiring
    rather than on its output.
    """
    global _provider, _enabled

    endpoint = endpoint or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    if not endpoint and exporter is None:
        _enabled = False
        return lambda: None

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.propagate import set_global_textmap
    from opentelemetry.propagators.textmap import default_getter  # noqa: F401
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    set_global_textmap(TraceContextTextMapPropagator())
    resource = Resource.create({
        "service.name": service_name,
        "service.version": _version(),
    })
    provider = TracerProvider(resource=resource)
    if exporter is None:
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint.rstrip("/") + "/v1/traces"))
        )
    else:
        # An explicitly supplied sink is one the caller means to read back, so it is
        # attached synchronously. Batching it would make every read a race with the
        # exporter's own timer.
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _provider = provider
    _enabled = True

    def shutdown() -> None:
        global _enabled
        provider.shutdown()
        _enabled = False

    return shutdown


def _version() -> str:
    try:
        from importlib.metadata import version

        return version("gpu-broker")
    except Exception:  # pragma: no cover - only when running from a bare checkout
        return "0"


def flush(timeout_ms: int = 5_000) -> None:
    """Push queued spans now. The CLI is short-lived; a batch processor that
    only flushes on its own timer loses the last trace of every run."""
    if _provider is not None:
        _provider.force_flush(timeout_ms)


# --- spans ------------------------------------------------------------------

class _NoSpan:
    """What a span body talks to when tracing is off. Same surface, no cost."""

    __slots__ = ()

    def set_attribute(self, *_a, **_k) -> None: ...
    def set_attributes(self, *_a, **_k) -> None: ...
    def failed(self, *_a, **_k) -> None: ...
    def add_event(self, *_a, **_k) -> None: ...
    def update_name(self, *_a, **_k) -> None: ...

    @property
    def trace_id(self) -> str | None:
        return None


class _Span:
    """A handle that can say the work failed without raising.

    Needed because the interesting failures here do not propagate. The
    scheduler catches `BackendError` and moves the job to FAILED; a span that
    inferred its outcome from "did this block raise" would record ok for a
    dispatch that could not get a machine. A span that reports success for a
    failed dependency is worse than no span.
    """

    __slots__ = ("_span",)

    def __init__(self, span) -> None:
        self._span = span

    def set_attribute(self, key: str, value) -> None:
        for k, v in sanitise({key: value}).items():
            self._span.set_attribute(k, v)

    def set_attributes(self, attrs: dict) -> None:
        for k, v in sanitise(attrs).items():
            self._span.set_attribute(k, v)

    def add_event(self, name: str, attrs: dict | None = None) -> None:
        self._span.add_event(name, sanitise(attrs or {}))

    def update_name(self, name: str) -> None:
        self._span.update_name(name)

    def failed(self, outcome: str, **attrs) -> None:
        from opentelemetry.trace import Status, StatusCode

        self._span.set_status(Status(StatusCode.ERROR, outcome))
        self.set_attributes({"outcome": outcome, **attrs})

    @property
    def trace_id(self) -> str | None:
        ctx = self._span.get_span_context()
        return format(ctx.trace_id, "032x") if ctx.is_valid else None


@contextlib.contextmanager
def span(name: str, *, kind: str = "internal", parent: str | None = None, **attrs):
    """One timed unit of work.

    `parent` is a W3C traceparent string, for the consumer side of the queue
    hop where there is no ambient context to inherit from.
    """
    if not _enabled:
        yield _NoSpan()
        return

    from opentelemetry import trace
    from opentelemetry.trace import SpanKind

    kinds = {
        "internal": SpanKind.INTERNAL,
        "server": SpanKind.SERVER,
        "client": SpanKind.CLIENT,
        "producer": SpanKind.PRODUCER,
        "consumer": SpanKind.CONSUMER,
    }
    context = _context_from(parent) if parent else None
    tracer = trace.get_tracer("gpu_broker")
    with tracer.start_as_current_span(
        name, context=context, kind=kinds[kind], attributes=sanitise(attrs),
        # Both off, and both replaced below. The SDK's own versions put the
        # exception's `str()` straight onto the span, and an exception message
        # is a string this module has never seen: a DSN, a signed URL, a shell
        # line echoed back by a backend. It has to go through the scrubber like
        # everything else, and leaving the SDK's copy on as well would record
        # the unscrubbed message next to the scrubbed one.
        record_exception=False, set_status_on_exception=False,
    ) as raw:
        handle = _Span(raw)
        try:
            yield handle
        except BaseException as exc:
            from opentelemetry.trace import Status, StatusCode

            raw.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            raw.add_event("exception", sanitise({
                "exception.type": type(exc).__name__,
                "exception.message": str(exc),
            }))
            raise


def current_trace_id() -> str | None:
    if not _enabled:
        return None
    from opentelemetry import trace

    ctx = trace.get_current_span().get_span_context()
    return format(ctx.trace_id, "032x") if ctx.is_valid else None


# --- the queue hop ----------------------------------------------------------

def current_traceparent() -> str | None:
    """The ambient context as one header value, or None when tracing is off."""
    if not _enabled:
        return None
    from opentelemetry.propagate import inject

    carrier: dict[str, str] = {}
    inject(carrier)
    return carrier.get("traceparent")


def _context_from(traceparent: str):
    from opentelemetry.propagate import extract

    return extract({"traceparent": traceparent})


def record_queue_context(conn: sqlite3.Connection, job_id: str, now: dt.datetime) -> None:
    """Park the submitting process's context on the job it just queued.

    Deliberately outside the admission transaction and deliberately swallowing
    its own errors: a job that was admitted must stay admitted even if the
    telemetry write fails. An untraceable job is a worse day; an unadmitted one
    is a bug.
    """
    traceparent = current_traceparent()
    if not traceparent:
        return
    try:
        with contextlib.closing(conn.cursor()) as cursor:
            cursor.execute(
                "INSERT OR REPLACE INTO trace_context (job_id, traceparent, at) "
                "VALUES (?, ?, ?)",
                (job_id, traceparent, now.isoformat()),
            )
        if not conn.in_transaction:
            conn.commit()
    except sqlite3.Error:
        pass


def queue_context(conn: sqlite3.Connection, job_id: str) -> str | None:
    """The traceparent the submitting process parked, if it is still there."""
    try:
        row = conn.execute(
            "SELECT traceparent FROM trace_context WHERE job_id = ?", (job_id,)
        ).fetchone()
    except sqlite3.Error:
        return None
    return row["traceparent"] if row else None
