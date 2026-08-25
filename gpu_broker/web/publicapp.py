"""A separate ASGI app holding the public status page, and nothing else.

The point of putting this in its own app rather than adding one more route to
the main one is that *read-only* becomes a property of the route table instead
of a property of every future change to it. There is no submit handler here to
forget to guard, no cancel, no admin. `test_public_app_has_no_mutating_routes`
walks the routes and asserts it, so adding a POST here fails the suite.

It is mounted into the club app at /status for convenience, and can also be
served on its own so the public URL and the credentialed app are not even the
same process.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from collections.abc import Callable
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .public import PublicStatus, build

HERE = Path(__file__).parent


class _Cache:
    """One entry, held for a few seconds.

    A status page exists to be linked, and a link that gets attention arrives as
    a burst. Rebuilding the aggregates per request means a hundred readers turn
    into a hundred full table scans against the same SQLite file the scheduler
    is trying to write to. The stale window is the price and it is small.
    """

    def __init__(self, seconds: float, now: Callable[[], dt.datetime]) -> None:
        self.seconds = seconds
        self.now = now
        self._at: dt.datetime | None = None
        self._value: PublicStatus | None = None

    def get(self, produce: Callable[[], PublicStatus]) -> PublicStatus:
        moment = self.now()
        if (
            self._value is not None
            and self._at is not None
            and (moment - self._at).total_seconds() < self.seconds
        ):
            return self._value
        self._value = produce()
        self._at = moment
        return self._value

    def clear(self) -> None:
        self._at = None
        self._value = None


def create_public_app(
    open_broker: Callable[[], object],
    *,
    title: str = "GPU broker",
    cache_seconds: float = 15.0,
    repo_url: str = "https://github.com/lgoyal6/gpu-broker",
) -> FastAPI:
    """An app with exactly two GET routes and no way to change anything."""
    app = FastAPI(
        title=f"{title} status", docs_url=None, redoc_url=None, openapi_url=None
    )

    templates = Jinja2Templates(directory=str(HERE / "templates"))
    from .app import _duration, _money, _sparkline

    templates.env.filters["money"] = _money
    templates.env.filters["duration"] = _duration
    templates.env.filters["sparkline"] = _sparkline

    app.mount("/static", StaticFiles(directory=str(HERE / "static")), name="static")
    app.state.cache = None
    app.state.title = title

    def status_now() -> PublicStatus:
        with open_broker() as broker:  # type: ignore[attr-defined]
            if app.state.cache is None:
                app.state.cache = _Cache(cache_seconds, broker.clock.now)
            return app.state.cache.get(lambda: build(broker))

    @app.get("/", response_class=HTMLResponse)
    def status_page(request: Request) -> HTMLResponse:
        status = status_now()
        response = templates.TemplateResponse(
            request,
            "status.html",
            {"status": status, "title": title, "repo_url": repo_url},
        )
        # Survives being linked: a shared cache may serve the same render to
        # everyone for the window, and may keep serving a stale one while it
        # refetches rather than dropping the page during a burst.
        response.headers["Cache-Control"] = (
            f"public, max-age={int(cache_seconds)}, stale-while-revalidate=60"
        )
        return response

    @app.get("/status.json")
    def status_json() -> JSONResponse:
        """The same aggregates, for anybody who would rather not scrape HTML.

        Same object as the page, so it cannot drift into exposing more. Sits
        under whatever prefix the app is mounted at: `/status.json` when this
        app is served on its own, `/status/status.json` when it is mounted into
        the club app at /status. The page links to it via `root_path` so the
        link is right either way.
        """
        payload = dataclasses.asdict(status_now())
        response = JSONResponse(_jsonable(payload))
        response.headers["Cache-Control"] = f"public, max-age={int(cache_seconds)}"
        return response

    return app


def _jsonable(value):
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return value.total_seconds()
    if hasattr(value, "quantize"):  # Decimal, as a string: JSON floats lose cents
        return str(value)
    return value
