"""The web app.

Server-rendered HTML, plain forms, and about thirty lines of vanilla JavaScript
for the live log tail. No build step, no `node_modules`, no CDN. Other club
members have to be able to read this, and the moment it needs `npm install` to
change a label it stops being something anyone else touches.

A fresh database connection per request. SQLite connections belong to one
thread, FastAPI runs sync endpoints in a threadpool, and opening a connection is
microseconds. Sharing one would be faster and wrong.
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..broker import Broker
from ..clock import SystemClock
from ..config import BrokerConfig, load_config
from ..errors import BrokerError
from ..money import Currency, fmt
from ..states import JobState
from .auth import (
    SESSION_COOKIE,
    STATE_COOKIE,
    AuthError,
    DevProvider,
    GitHubProvider,
    Identity,
    IdentityProvider,
    Membership,
    Sessions,
    WebConfig,
)

HERE = Path(__file__).parent


def open_broker(config: BrokerConfig) -> Broker:
    """A broker that cannot touch a machine.

    `backends=[]` is the whole security posture of this process: no AWS client
    is constructed, no SSH key is read, and nothing here can launch or terminate
    anything. The `gpu run` daemon does all of that.
    """
    return Broker.open(config.db_path.parent, clock=SystemClock(), backends=[], config=config)


def create_app(
    config: BrokerConfig | None = None,
    web: WebConfig | None = None,
    provider: IdentityProvider | None = None,
    web_overrides: dict | None = None,
) -> FastAPI:
    config = config or load_config()
    web = web or config.web
    if web_overrides:
        # `gpu web --public` turning on the status page for one run, without
        # editing the config file to do it.
        web = dataclasses.replace(web, **web_overrides)
    web.validate()

    sessions = Sessions(web.resolved_session_secret(), web.session_max_age_seconds)
    membership = Membership(web)
    if provider is None:
        provider = DevProvider(web.dev_login) if web.dev_login else GitHubProvider(web)

    app = FastAPI(title="gpu-broker", docs_url=None, redoc_url=None)
    app.state.config = config
    app.state.web = web
    app.state.sessions = sessions
    app.state.membership = membership
    app.state.provider = provider

    templates = Jinja2Templates(directory=str(HERE / "templates"))
    templates.env.filters["money"] = _money
    templates.env.filters["ago"] = _ago
    templates.env.filters["duration"] = _duration
    templates.env.filters["sparkline"] = _sparkline
    app.state.templates = templates
    app.mount("/static", StaticFiles(directory=str(HERE / "static")), name="static")

    if web.public_status:
        # Mounted, not written inline: the public page lives in an app whose
        # route table has no way to change anything. See web/publicapp.py.
        from .publicapp import create_public_app

        app.mount(
            "/status",
            create_public_app(
                lambda: open_broker(config),
                title=web.public_status_title,
                cache_seconds=web.public_status_cache_seconds,
            ),
            name="status",
        )

    # ------------------------------------------------------------ plumbing

    def broker_for(request: Request) -> Broker:
        instance = open_broker(request.app.state.config)
        try:
            yield instance
        finally:
            instance.store.conn.close()

    def viewer(request: Request) -> Identity:
        identity = request.app.state.sessions.read(request.cookies.get(SESSION_COOKIE))
        if identity is None:
            raise HTTPException(status_code=401, detail="sign in")
        return identity

    def page(request: Request, name: str, status_code: int = 200, **context) -> HTMLResponse:
        identity = request.app.state.sessions.read(request.cookies.get(SESSION_COOKIE))
        return request.app.state.templates.TemplateResponse(
            request,
            name,
            {
                "viewer": identity,
                "is_admin": bool(identity and membership.is_admin(identity.login)),
                **context,
            },
            # Rendering a friendly page must not turn a 403 into a 200. Anything
            # scripting against this -- a health check, a browser's own error
            # handling, a member piping it to `jq` -- would be told it worked.
            status_code=status_code,
        )

    app.state.page = page

    @app.exception_handler(401)
    async def needs_sign_in(request: Request, exc: HTTPException):
        if request.url.path == "/metrics":
            # A scraper cannot follow a redirect to a sign-in page, and turning
            # its 401 into a 303 makes the failure look like success.
            from fastapi.responses import PlainTextResponse

            return PlainTextResponse(str(exc.detail), status_code=401)
        return RedirectResponse("/auth/login", status_code=303)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException):
        if request.url.path == "/metrics":
            from fastapi.responses import PlainTextResponse

            return PlainTextResponse(str(exc.detail), status_code=exc.status_code)
        if exc.status_code == 401:
            return RedirectResponse("/auth/login", status_code=303)
        return page(
            request, "error.html", status_code=exc.status_code,
            message=exc.detail, code=exc.status_code,
        )

    # ---------------------------------------------------------------- auth

    @app.get("/auth/login")
    def login(request: Request) -> Any:
        state = Sessions.new_state()
        response = RedirectResponse(request.app.state.provider.authorize_url(state), status_code=303)
        response.set_cookie(
            STATE_COOKIE, state, httponly=True, samesite="lax", max_age=600
        )
        return response

    @app.get("/auth/callback")
    def callback(request: Request, code: str = "", state: str = "") -> Any:
        expected = request.cookies.get(STATE_COOKIE)
        if not expected or state != expected:
            # Without this, anybody can hand somebody a link that signs them
            # into an account they did not choose.
            return page(
                request,
                "error.html",
                status_code=400,
                code=400,
                message="that sign-in link has expired or was not started here. Try again.",
            )
        try:
            identity, org_member = request.app.state.provider.exchange(code)
            request.app.state.membership.check(identity, org_member)
        except AuthError as exc:
            return page(request, "error.html", status_code=403, code=403, message=str(exc))

        with open_broker(request.app.state.config) as instance:
            # First sign-in creates the account with the default budget, so
            # nobody has to be added by hand before they can do anything.
            if instance.store.maybe_user(identity.login) is None:
                instance.add_user(identity.login, display_name=identity.display)

        response = RedirectResponse("/", status_code=303)
        response.set_cookie(
            SESSION_COOKIE,
            request.app.state.sessions.issue(identity),
            httponly=True,
            samesite="lax",
            max_age=request.app.state.web.session_max_age_seconds,
        )
        response.delete_cookie(STATE_COOKIE)
        return response

    @app.get("/auth/logout")
    def logout() -> Any:
        response = RedirectResponse("/auth/login", status_code=303)
        response.delete_cookie(SESSION_COOKIE)
        return response

    # ----------------------------------------------------------- dashboard

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request, me: Identity = Depends(viewer),
                  broker: Broker = Depends(broker_for)) -> Any:
        now = broker.clock.now()
        holders = broker.who()
        since = now - dt.timedelta(hours=24)
        from ..metrics import QUEUE_DEPTH, UTILIZATION

        return page(
            request,
            "dashboard.html",
            utilization_series=broker.metrics.bucketed(UTILIZATION, since, now),
            queue_series=broker.metrics.bucketed(QUEUE_DEPTH, since, now),
            series_hours=24,
            holders=[
                {
                    "job": job,
                    "elapsed": job.elapsed_hours(now),
                    "spent": broker.job_spend(job.job_id),
                    "utilization": _recent_utilization(broker, job.job_id),
                }
                for job in holders
            ],
            queue=broker.queue()[:10],
            queue_depth=len(broker.store.queued_jobs()),
            forecasts=[broker.forecast(currency) for currency in Currency],
            idle=broker.idle_jobs(),
            hosts=broker.hosts(),
            my_balances=broker.budgets(me.login),
        )

    # ---------------------------------------------------------------- jobs

    @app.get("/jobs", response_class=HTMLResponse)
    def jobs(request: Request, me: Identity = Depends(viewer),
             broker: Broker = Depends(broker_for), everyone: bool = False) -> Any:
        who = None if everyone else me.login
        history = broker.history(user_id=who, limit=100)
        return page(
            request,
            "jobs.html",
            jobs=[
                {
                    "job": job,
                    "spent": broker.job_spend(job.job_id),
                    "utilization": _peak_utilization(broker, job.job_id),
                }
                for job in history
            ],
            everyone=everyone,
        )

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    def job_detail(request: Request, job_id: str, me: Identity = Depends(viewer),
                   broker: Broker = Depends(broker_for)) -> Any:
        try:
            job = broker.status(job_id)
        except BrokerError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        samples = broker.store.samples_for(job.job_id, limit=240)
        return page(
            request,
            "job.html",
            job=job,
            spent=broker.job_spend(job.job_id),
            history=broker.store.history(job.job_id),
            logs=broker.logs(job.job_id)[-400:],
            samples=samples,
            peak=max((s.gpu_percent for s in samples), default=None),
            notices=broker.store.notifications_for_job(job.job_id),
            mine=job.user_id == me.login,
        )

    @app.post("/jobs/{job_id}/cancel")
    def cancel(request: Request, job_id: str, me: Identity = Depends(viewer),
               broker: Broker = Depends(broker_for)) -> Any:
        try:
            job = broker.status(job_id)
        except BrokerError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        if job.user_id != me.login and not membership.is_admin(me.login):
            raise HTTPException(status_code=403, detail=f"that job belongs to {job.user_id}")
        if job.is_terminal:
            raise HTTPException(status_code=400, detail=f"already {job.state.lower()}")

        if job.state is JobState.QUEUED:
            # No machine involved, so this page can finish the job itself.
            broker.cancel(job.job_id, actor=me.login)
        else:
            # This process holds no credentials. Record the request; the daemon
            # stops the machine on its next tick.
            broker.store.request_cancel(job, me.login)
        return RedirectResponse(f"/jobs/{job.job_id}", status_code=303)

    # -------------------------------------------------------------- submit

    @app.get("/submit", response_class=HTMLResponse)
    def submit_form(request: Request, me: Identity = Depends(viewer),
                    broker: Broker = Depends(broker_for)) -> Any:
        return page(
            request,
            "submit.html",
            gpus=broker.config.gpu_types,
            prices=broker.prices.all(),
            balances=broker.budgets(me.login),
            allowance=broker.config.startup_allowance_hours,
            environments=broker.store.environments(),
        )

    @app.post("/submit")
    def submit(request: Request, me: Identity = Depends(viewer),
               broker: Broker = Depends(broker_for),
               command: str = Form(...), gpu: str = Form("a10g"),
               hours: float = Form(1.0), budget: str = Form(""),
               environment: str = Form("")) -> Any:
        try:
            result = broker.submit(
                user_id=me.login,
                command=command.strip(),
                gpu_type=gpu,
                hours=hours,
                budget=Decimal(budget.lstrip("$")) if budget.strip() else None,
                environment=environment.strip() or None,
            )
        except (BrokerError, ArithmeticError) as exc:
            return page(
                request, "submit.html", error=str(exc), gpus=broker.config.gpu_types,
                prices=broker.prices.all(), balances=broker.budgets(me.login),
                allowance=broker.config.startup_allowance_hours,
                environments=broker.store.environments(),
                command=command, gpu=gpu, hours=hours, budget=budget,
                environment=environment,
            )
        if not result.accepted:
            return page(
                request, "submit.html", refusal=result.refusal.reason,
                gpus=broker.config.gpu_types, prices=broker.prices.all(),
                balances=broker.budgets(me.login),
                allowance=broker.config.startup_allowance_hours,
                environments=broker.store.environments(),
                command=command, gpu=gpu, hours=hours, budget=budget,
                environment=environment,
            )
        return RedirectResponse(f"/jobs/{result.job.job_id}", status_code=303)

    # ----------------------------------------------------------- live logs

    @app.get("/jobs/{job_id}/stream")
    async def stream(request: Request, job_id: str) -> Any:
        identity = request.app.state.sessions.read(request.cookies.get(SESSION_COOKIE))
        if identity is None:
            raise HTTPException(status_code=401, detail="sign in")

        async def events():
            after = int(request.query_params.get("after", 0))
            idle_rounds = 0
            while True:
                if await request.is_disconnected():
                    return
                lines, done = await asyncio.to_thread(_poll_logs, config, job_id, after)
                for entry_id, at, stream_name, line in lines:
                    after = entry_id
                    yield _sse(
                        "line",
                        {
                            "id": entry_id,
                            "at": at.strftime("%H:%M:%S"),
                            "stream": stream_name,
                            "line": line,
                        },
                    )
                if done:
                    yield _sse("done", {"state": done})
                    return
                idle_rounds = 0 if lines else idle_rounds + 1
                # A comment frame keeps proxies from closing a quiet connection.
                if idle_rounds and idle_rounds % 15 == 0:
                    yield ": keepalive\n\n"
                await asyncio.sleep(1.0)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # --------------------------------------------------------------- admin

    @app.get("/admin", response_class=HTMLResponse)
    def admin(request: Request, me: Identity = Depends(viewer),
              broker: Broker = Depends(broker_for)) -> Any:
        if not membership.is_admin(me.login):
            raise HTTPException(status_code=403, detail="that page is for club officers")
        return page(
            request,
            "admin.html",
            users=[
                {"user": user, "balances": broker.budgets(user.user_id)}
                for user in broker.users()
            ],
            hosts=broker.hosts(),
            pool=broker.pool(),
        )

    @app.post("/admin/budget")
    def set_budget(request: Request, me: Identity = Depends(viewer),
                   broker: Broker = Depends(broker_for),
                   user_id: str = Form(...), usd: str = Form(""),
                   gpu_hours: str = Form("")) -> Any:
        if not membership.is_admin(me.login):
            raise HTTPException(status_code=403, detail="that page is for club officers")
        try:
            broker.add_user(
                user_id,
                budget_usd=Decimal(usd.lstrip("$")) if usd.strip() else None,
                budget_gpu_hours=Decimal(gpu_hours) if gpu_hours.strip() else None,
            )
        except (BrokerError, ArithmeticError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/hosts/{hostname}/drain")
    def drain(request: Request, hostname: str, me: Identity = Depends(viewer),
              broker: Broker = Depends(broker_for), reason: str = Form("maintenance"),
              undrain: str = Form("")) -> Any:
        if not membership.is_admin(me.login):
            raise HTTPException(status_code=403, detail="that page is for club officers")
        try:
            if undrain:
                broker.undrain_host(hostname)
            else:
                broker.drain_host(hostname, reason, actor=me.login)
        except BrokerError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return RedirectResponse("/admin", status_code=303)

    @app.get("/metrics")
    def prometheus_metrics(request: Request, broker: Broker = Depends(broker_for)) -> Any:
        """The scrape endpoint.

        Off unless a token is configured. These series carry usernames and job
        ids, and an endpoint that lists who is running what is not a thing to
        leave open because Prometheus conventionally does.
        """
        from ..metrics import prometheus

        token = request.app.state.web.resolved_metrics_token()
        if not token:
            raise HTTPException(
                status_code=404,
                detail=(
                    "the metrics endpoint is off. Set web.metrics_token, or "
                    "GPU_BROKER_METRICS_TOKEN, to turn it on -- these series name "
                    "people and jobs"
                ),
            )
        offered = request.headers.get("authorization", "")
        if offered != f"Bearer {token}":
            raise HTTPException(status_code=401, detail="bad or missing bearer token")

        from fastapi.responses import PlainTextResponse

        return PlainTextResponse(
            prometheus(broker.metrics), media_type="text/plain; version=0.0.4"
        )

    @app.get("/report", response_class=HTMLResponse)
    def report_page(request: Request, me: Identity = Depends(viewer),
                    broker: Broker = Depends(broker_for), days: float = 90.0) -> Any:
        from ..report import markdown

        return page(
            request,
            "report.html",
            body=markdown(broker.report(since=broker.clock.now() - dt.timedelta(days=days))),
            days=days,
        )

    @app.get("/health")
    def health() -> dict:
        return {"ok": True}

    return app


# --- helpers ----------------------------------------------------------------


def _poll_logs(config: BrokerConfig, job_id: str, after: int):
    """One read, in a worker thread. SQLite connections belong to one thread."""
    with open_broker(config) as instance:
        try:
            job = instance.status(job_id)
        except BrokerError:
            return [], "gone"
        lines = instance.store.read_logs(job.job_id, after_id=after, limit=200)
        return lines, str(job.state) if job.is_terminal else ""


def _sse(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


def _recent_utilization(broker: Broker, job_id: str) -> float | None:
    samples = broker.store.samples_for(job_id, limit=5)
    if not samples:
        return None
    return sum(sample.gpu_percent for sample in samples) / len(samples)


def _peak_utilization(broker: Broker, job_id: str) -> float | None:
    samples = broker.store.samples_for(job_id, limit=500)
    return max((sample.gpu_percent for sample in samples), default=None)


def _sparkline(values: list[float | None], height: int = 34, ceiling: float | None = None) -> str:
    """An inline SVG polyline. No chart library, no build step.

    Gaps stay gaps: a run of `None` breaks the line rather than dropping it to
    zero, because "nothing was running" and "a GPU sat idle" are different
    claims and only one of them is a problem.
    """
    points = [v for v in values if v is not None]
    if not points:
        return ""
    top = ceiling if ceiling is not None else max(max(points), 1.0)
    width = max(len(values) * 6, 60)
    step = width / max(len(values) - 1, 1)

    segments: list[list[str]] = []
    current: list[str] = []
    for index, value in enumerate(values):
        if value is None:
            if current:
                segments.append(current)
            current = []
            continue
        x = index * step
        y = height - (min(value, top) / top) * (height - 2) - 1
        current.append(f"{x:.1f},{y:.1f}")
    if current:
        segments.append(current)
    if not segments:
        return ""

    # A run of one is drawn as a dot, not dropped. A pool that is used twice a
    # day has every sample isolated between gaps, and a polyline needs two
    # points -- so the honest-looking chart would be a blank panel.
    marks = []
    for segment in segments:
        if len(segment) > 1:
            marks.append(
                f'<polyline points="{" ".join(segment)}" fill="none" '
                f'stroke="currentColor" stroke-width="1.5" '
                f'stroke-linejoin="round" stroke-linecap="round"/>'
            )
        else:
            x, y = segment[0].split(",")
            marks.append(f'<circle cx="{x}" cy="{y}" r="1.6" fill="currentColor"/>')
    return (
        f'<svg viewBox="0 0 {width:.0f} {height}" preserveAspectRatio="none" '
        f'width="100%" height="{height}" role="img">{"".join(marks)}</svg>'
    )


def _money(value, currency=Currency.USD) -> str:
    return fmt(value, currency)


def _ago(moment: dt.datetime | None) -> str:
    if moment is None:
        return "-"
    seconds = (dt.datetime.now(dt.timezone.utc) - moment).total_seconds()
    if seconds < 90:
        return "just now"
    if seconds < 5400:
        return f"{seconds / 60:.0f}m ago"
    if seconds < 172800:
        return f"{seconds / 3600:.0f}h ago"
    return f"{seconds / 86400:.0f}d ago"


def _duration(hours: float | None) -> str:
    if not hours:
        return "-"
    if hours < 1:
        return f"{hours * 60:.0f}m"
    return f"{hours:.1f}h"
