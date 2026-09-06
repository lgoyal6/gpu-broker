"""Who is using this, and are they in the club.

Two ways to answer the second question, because clubs differ: a GitHub org, if
one exists, or a list of logins somebody maintains. The org is less work and
goes stale less; the allowlist works when there is no org, which is most clubs
in their first year.

Identity is behind an interface so the tests drive the real routes without
talking to GitHub. That matters more than usual here: an auth path that is only
exercised by hand is one where the interesting cases -- expired state, a login
that is not a member, a revoked token -- are never exercised at all.
"""

from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from ..errors import BrokerError, ConfigError

SESSION_COOKIE = "gpu_broker_session"
STATE_COOKIE = "gpu_broker_oauth_state"
GITHUB_AUTHORIZE = "https://github.com/login/oauth/authorize"
GITHUB_TOKEN = "https://github.com/login/oauth/access_token"
GITHUB_API = "https://api.github.com"


class AuthError(BrokerError):
    """The sign-in did not work. The message is shown to the person."""


@dataclass(frozen=True)
class Identity:
    login: str
    name: str = ""
    avatar_url: str = ""

    @property
    def display(self) -> str:
        return self.name or self.login


@dataclass(frozen=True)
class WebConfig:
    """Everything the web app needs that is not already broker config."""

    base_url: str = "http://localhost:8000"
    github_client_id: str = ""
    github_client_secret: str = ""
    """Read from GPU_BROKER_GITHUB_CLIENT_SECRET if not set here. Putting a
    secret in a file that lives next to the database is how it ends up in a
    screenshot."""
    github_org: str = ""
    allowlist: tuple[str, ...] = ()
    allowlist_file: str = ""
    session_secret: str = ""
    session_max_age_seconds: int = 60 * 60 * 24 * 14
    admins: tuple[str, ...] = ()
    public_status: bool = False
    """Serve an unauthenticated /status page.

    Off by default. It publishes how much the club is spending and how busy the
    pool is, which is the point when you want to show somebody the thing is
    real, and not something to start doing because a config default said so.
    Usernames never appear on it -- see `gpu_broker.web.public`."""
    public_status_title: str = "GPU broker"
    public_status_cache_seconds: float = 15.0
    """How long one render is reused. A page that exists to be linked gets read
    in bursts, and rebuilding the aggregates per reader turns one link into a
    hundred table scans against the file the scheduler is writing to."""

    metrics_token: str = ""
    """Bearer token for `/metrics`. Unset means the endpoint is off, because the
    series carry usernames and job ids and a scrape endpoint is not something to
    leave open by accident. Read from GPU_BROKER_METRICS_TOKEN if not set here."""

    dev_login: str = ""
    """Skips OAuth entirely and signs everyone in as this login. For working on
    the app locally without registering an OAuth app. Refused unless the base
    URL is localhost, because leaving this on in production is an open door."""

    def resolved_secret(self) -> str:
        return self.github_client_secret or os.environ.get(
            "GPU_BROKER_GITHUB_CLIENT_SECRET", ""
        )

    def resolved_metrics_token(self) -> str:
        return self.metrics_token or os.environ.get("GPU_BROKER_METRICS_TOKEN", "")

    def resolved_session_secret(self) -> str:
        secret = self.session_secret or os.environ.get("GPU_BROKER_SESSION_SECRET", "")
        if secret:
            return secret
        # A generated secret means sessions do not survive a restart, which is a
        # papercut. A hardcoded default would mean anybody could forge one.
        return secrets.token_urlsafe(32)

    def validate(self) -> None:
        if self.dev_login and not (
            self.base_url.startswith("http://localhost")
            or self.base_url.startswith("http://127.0.0.1")
        ):
            raise ConfigError(
                f"web.dev_login is set but base_url is {self.base_url!r}. That would "
                "sign in anybody who found the page. Only allowed on localhost"
            )
        if not self.dev_login and not self.github_client_id:
            raise ConfigError(
                "web.github_client_id is not set. Register an OAuth app at "
                "https://github.com/settings/developers, or set web.dev_login "
                "to work on this locally without one"
            )
        if (
            not self.dev_login
            and not self.github_org
            and not self.allowlist
            and not self.allowlist_file
        ):
            # Not required in dev mode: there is one login, it is the one you
            # configured, and making you also allowlist it is a hurdle for the
            # exact situation this mode exists to remove.
            raise ConfigError(
                "nobody could sign in: set web.github_org, or web.allowlist, or "
                "web.allowlist_file"
            )


def load_web_config(raw: dict | None) -> WebConfig:
    if not raw:
        return WebConfig()
    known = set(WebConfig.__dataclass_fields__)
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(
            f"unknown keys under 'web' in config.json: {', '.join(sorted(unknown))}. "
            f"Known: {', '.join(sorted(known))}"
        )
    settings = dict(raw)
    for key in ("allowlist", "admins"):
        if key in settings:
            settings[key] = tuple(settings[key])
    from dataclasses import replace

    return replace(WebConfig(), **settings)


# --- membership -------------------------------------------------------------


class Membership:
    """Whether a GitHub login is in the club.

    Checked on every sign-in rather than once at account creation. Somebody who
    leaves the org, or comes off the list, stops being able to spend the club's
    credits at their next sign-in rather than never.
    """

    def __init__(self, config: WebConfig) -> None:
        self.config = config

    def allowed_logins(self) -> set[str]:
        logins = {login.lower() for login in self.config.allowlist}
        if self.config.dev_login:
            logins.add(self.config.dev_login.lower())
        if self.config.allowlist_file:
            path = Path(self.config.allowlist_file).expanduser()
            if path.is_file():
                for line in path.read_text().splitlines():
                    entry = line.split("#", 1)[0].strip()
                    if entry:
                        logins.add(entry.lower())
        return logins

    def check(self, identity: Identity, org_member: bool) -> None:
        """Raises AuthError with something the person can act on."""
        if self.config.github_org and org_member:
            return
        if identity.login.lower() in self.allowed_logins():
            return

        if self.config.github_org:
            raise AuthError(
                f"{identity.login} is not a public member of the "
                f"{self.config.github_org} organisation. Ask an officer to add you, "
                "and make sure your membership is set to public in your GitHub "
                "organisation settings -- private membership is invisible to us"
            )
        raise AuthError(
            f"{identity.login} is not on the club allowlist. Ask an officer to add you"
        )

    def recheck(self, login: str) -> None:
        """The same question, asked on every request instead of once.

        `check` runs at sign-in. A session lasts fourteen days, so on its own it
        means somebody removed from the club keeps spending for two weeks.

        What can be re-asked offline is the allowlist: it is a file or a config
        value, and reading it costs a stat. Org membership cannot -- answering
        it needs the person's GitHub token, which is exchanged once at sign-in
        and deliberately not kept. So a deployment configured against an org
        only gets the check at sign-in, and the enforcement that does work there
        is suspension in the users table, which `gpu run` can also see.

        Raises AuthError, the same as `check`, so callers handle one thing.
        """
        if self.config.github_org:
            # An org member need not be on the allowlist at all -- `check` lets
            # them in on the org alone -- so an allowlist miss here would prove
            # nothing and would lock out everybody who signed in that way.
            return
        if login.lower() in self.allowed_logins():
            return
        raise AuthError(
            f"{login} is no longer on the club allowlist. "
            "Ask an officer to add you back"
        )

    def is_admin(self, login: str) -> bool:
        return login.lower() in {admin.lower() for admin in self.config.admins}


# --- identity providers -----------------------------------------------------


@runtime_checkable
class IdentityProvider(Protocol):
    def authorize_url(self, state: str) -> str: ...

    def exchange(self, code: str) -> tuple[Identity, bool]:
        """Returns the identity and whether they are in the configured org."""
        ...


class GitHubProvider:
    def __init__(self, config: WebConfig, client: Any | None = None) -> None:
        self.config = config
        self._client = client

    def _http(self) -> Any:
        if self._client is not None:
            return self._client
        import httpx

        return httpx.Client(timeout=10.0)

    def authorize_url(self, state: str) -> str:
        from urllib.parse import urlencode

        scope = "read:org" if self.config.github_org else "read:user"
        query = urlencode(
            {
                "client_id": self.config.github_client_id,
                "redirect_uri": f"{self.config.base_url.rstrip('/')}/auth/callback",
                "scope": scope,
                "state": state,
            }
        )
        return f"{GITHUB_AUTHORIZE}?{query}"

    def exchange(self, code: str) -> tuple[Identity, bool]:
        client = self._http()
        secret = self.config.resolved_secret()
        if not secret:
            raise AuthError(
                "the GitHub client secret is not configured. Set "
                "GPU_BROKER_GITHUB_CLIENT_SECRET in the environment"
            )

        response = client.post(
            GITHUB_TOKEN,
            data={
                "client_id": self.config.github_client_id,
                "client_secret": secret,
                "code": code,
                "redirect_uri": f"{self.config.base_url.rstrip('/')}/auth/callback",
            },
            headers={"Accept": "application/json"},
        )
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise AuthError(
                f"GitHub refused the sign-in: {payload.get('error_description', payload.get('error', 'no token'))}"
            )

        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
        user = client.get(f"{GITHUB_API}/user", headers=headers).json()
        login = user.get("login")
        if not login:
            raise AuthError("GitHub did not tell us who you are")

        org_member = False
        if self.config.github_org:
            check = client.get(
                f"{GITHUB_API}/orgs/{self.config.github_org}/members/{login}",
                headers=headers,
            )
            org_member = check.status_code == 204

        return (
            Identity(
                login=login,
                name=user.get("name") or login,
                avatar_url=user.get("avatar_url", ""),
            ),
            org_member,
        )


class DevProvider:
    """Signs everybody in as one login, for working on the app locally.

    Config refuses this unless the base URL is localhost.
    """

    def __init__(self, login: str) -> None:
        self.login = login

    def authorize_url(self, state: str) -> str:
        return f"/auth/callback?code=dev&state={state}"

    def exchange(self, code: str) -> tuple[Identity, bool]:
        return Identity(login=self.login, name=f"{self.login} (dev)"), True


# --- sessions ---------------------------------------------------------------


class Sessions:
    """Signed cookies. No server-side session store to keep or expire."""

    def __init__(self, secret: str, max_age: int) -> None:
        from itsdangerous import URLSafeTimedSerializer

        self._serializer = URLSafeTimedSerializer(secret, salt="gpu-broker-session")
        self.max_age = max_age

    def issue(self, identity: Identity) -> str:
        return self._serializer.dumps(
            {"login": identity.login, "name": identity.name, "avatar": identity.avatar_url}
        )

    def read(self, token: str | None) -> Identity | None:
        if not token:
            return None
        from itsdangerous import BadSignature, SignatureExpired

        try:
            data = self._serializer.loads(token, max_age=self.max_age)
        except (BadSignature, SignatureExpired):
            return None
        if not isinstance(data, dict) or not data.get("login"):
            return None
        return Identity(
            login=data["login"], name=data.get("name", ""), avatar_url=data.get("avatar", "")
        )

    @staticmethod
    def new_state() -> str:
        return secrets.token_urlsafe(24)
