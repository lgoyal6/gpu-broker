"""Signing in, and whether somebody is in the club.

An auth path exercised only by hand is one where the interesting cases -- a
forged cookie, a stale state parameter, a login that is not a member -- are
never exercised at all.
"""

from __future__ import annotations


import pytest

from gpu_broker.errors import ConfigError
from gpu_broker.web.auth import (
    AuthError,
    DevProvider,
    GitHubProvider,
    Identity,
    Membership,
    Sessions,
    WebConfig,
)


# ------------------------------------------------------------------ sessions


def test_a_session_round_trips():
    sessions = Sessions("secret", 3600)
    token = sessions.issue(Identity("ana", "Ana Lopez"))
    assert sessions.read(token) == Identity("ana", "Ana Lopez")


def test_a_forged_cookie_is_not_a_session():
    sessions = Sessions("secret", 3600)
    token = sessions.issue(Identity("ana"))
    assert sessions.read(token[:-6] + "AAAAAA") is None


def test_a_cookie_signed_with_another_secret_is_rejected():
    theirs = Sessions("their-secret", 3600).issue(Identity("mallory"))
    assert Sessions("our-secret", 3600).read(theirs) is None


def test_an_expired_session_is_rejected():
    sessions = Sessions("secret", max_age=-1)
    assert sessions.read(sessions.issue(Identity("ana"))) is None


def test_no_cookie_is_no_session():
    assert Sessions("secret", 3600).read(None) is None
    assert Sessions("secret", 3600).read("") is None


def test_state_values_are_not_guessable():
    assert len({Sessions.new_state() for _ in range(50)}) == 50


# ---------------------------------------------------------------- membership


def test_an_org_member_is_in():
    membership = Membership(WebConfig(github_org="ucsd-club"))
    membership.check(Identity("ana"), org_member=True)


def test_a_non_member_of_the_org_is_told_about_private_membership():
    """The commonest cause is somebody whose org membership is set to private,
    which is invisible to us and looks exactly like not being a member."""
    membership = Membership(WebConfig(github_org="ucsd-club"))
    with pytest.raises(AuthError, match="private membership is invisible"):
        membership.check(Identity("ana"), org_member=False)


def test_the_allowlist_works_without_an_org():
    membership = Membership(WebConfig(allowlist=("ana", "bo")))
    membership.check(Identity("ANA"), org_member=False)


def test_somebody_on_neither_is_refused_with_what_to_do():
    membership = Membership(WebConfig(allowlist=("ana",)))
    with pytest.raises(AuthError, match="Ask an officer"):
        membership.check(Identity("mallory"), org_member=False)


def test_an_allowlist_file_is_read_with_comments_and_blanks(tmp_path):
    listing = tmp_path / "members.txt"
    listing.write_text("# club roster\nana\n\n  bo  # treasurer\n")
    membership = Membership(WebConfig(allowlist_file=str(listing)))

    assert membership.allowed_logins() == {"ana", "bo"}
    membership.check(Identity("bo"), org_member=False)


def test_a_missing_allowlist_file_is_not_a_crash(tmp_path):
    membership = Membership(WebConfig(allowlist_file=str(tmp_path / "nope.txt")))
    assert membership.allowed_logins() == set()


def test_membership_is_checked_at_every_sign_in(tmp_path):
    """Somebody who comes off the list stops being able to spend the club's
    credits at their next sign-in, rather than never."""
    listing = tmp_path / "members.txt"
    listing.write_text("ana\nbo\n")
    membership = Membership(WebConfig(allowlist_file=str(listing)))
    membership.check(Identity("bo"), org_member=False)

    listing.write_text("ana\n")
    with pytest.raises(AuthError):
        membership.check(Identity("bo"), org_member=False)


def test_admins_are_named_explicitly():
    membership = Membership(WebConfig(admins=("Laksh",)))
    assert membership.is_admin("laksh")
    assert not membership.is_admin("ana")


# -------------------------------------------------------------------- config


def test_dev_login_is_refused_off_localhost():
    """Leaving it on in production signs in anybody who finds the page."""
    with pytest.raises(ConfigError, match="Only allowed on localhost"):
        WebConfig(dev_login="ana", base_url="https://gpu.ucsd.edu").validate()


def test_dev_login_is_fine_on_localhost():
    WebConfig(dev_login="ana", base_url="http://localhost:8000").validate()


def test_config_without_an_oauth_app_says_where_to_get_one():
    with pytest.raises(ConfigError, match="github.com/settings/developers"):
        WebConfig(allowlist=("ana",)).validate()


def test_config_where_nobody_could_sign_in_is_refused():
    with pytest.raises(ConfigError, match="nobody could sign in"):
        WebConfig(github_client_id="abc").validate()


def test_the_client_secret_comes_from_the_environment(monkeypatch):
    """A secret in a file that lives next to the database ends up in a
    screenshot."""
    monkeypatch.setenv("GPU_BROKER_GITHUB_CLIENT_SECRET", "from-env")
    assert WebConfig().resolved_secret() == "from-env"


# ------------------------------------------------------------------- GitHub


class FakeHttp:
    def __init__(self, token="tok", user=None, member_status=204):
        self.token = token
        self.user = user if user is not None else {"login": "ana", "name": "Ana"}
        self.member_status = member_status
        self.calls: list[tuple[str, str]] = []

    def post(self, url, **kwargs):
        self.calls.append(("POST", url))
        body = {"access_token": self.token} if self.token else {"error": "bad_verification_code"}
        return _Response(200, body)

    def get(self, url, **kwargs):
        self.calls.append(("GET", url))
        if url.endswith("/user"):
            return _Response(200, self.user)
        return _Response(self.member_status, {})


class _Response:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def test_the_authorize_url_carries_state_and_the_callback():
    provider = GitHubProvider(WebConfig(github_client_id="cid", base_url="https://gpu.ucsd.edu/"))
    url = provider.authorize_url("st4te")
    assert "client_id=cid" in url
    assert "state=st4te" in url
    assert "redirect_uri=https%3A%2F%2Fgpu.ucsd.edu%2Fauth%2Fcallback" in url


def test_org_membership_needs_the_org_scope():
    with_org = GitHubProvider(WebConfig(github_client_id="c", github_org="club")).authorize_url("s")
    without = GitHubProvider(WebConfig(github_client_id="c")).authorize_url("s")
    assert "read%3Aorg" in with_org
    assert "read%3Aorg" not in without


def test_a_successful_exchange_returns_who_and_whether_they_are_in_the_org(monkeypatch):
    monkeypatch.setenv("GPU_BROKER_GITHUB_CLIENT_SECRET", "shh")
    http = FakeHttp()
    provider = GitHubProvider(WebConfig(github_client_id="c", github_org="club"), client=http)

    identity, org_member = provider.exchange("code")
    assert identity == Identity("ana", "Ana", "")
    assert org_member is True


def test_a_204_is_the_only_thing_that_means_org_member(monkeypatch):
    monkeypatch.setenv("GPU_BROKER_GITHUB_CLIENT_SECRET", "shh")
    http = FakeHttp(member_status=404)
    provider = GitHubProvider(WebConfig(github_client_id="c", github_org="club"), client=http)
    assert provider.exchange("code")[1] is False


def test_github_refusing_the_code_is_reported_not_swallowed(monkeypatch):
    monkeypatch.setenv("GPU_BROKER_GITHUB_CLIENT_SECRET", "shh")
    provider = GitHubProvider(WebConfig(github_client_id="c"), client=FakeHttp(token=None))
    with pytest.raises(AuthError, match="bad_verification_code"):
        provider.exchange("code")


def test_a_missing_client_secret_says_which_variable(monkeypatch):
    monkeypatch.delenv("GPU_BROKER_GITHUB_CLIENT_SECRET", raising=False)
    provider = GitHubProvider(WebConfig(github_client_id="c"), client=FakeHttp())
    with pytest.raises(AuthError, match="GPU_BROKER_GITHUB_CLIENT_SECRET"):
        provider.exchange("code")


def test_the_org_check_is_skipped_when_there_is_no_org(monkeypatch):
    monkeypatch.setenv("GPU_BROKER_GITHUB_CLIENT_SECRET", "shh")
    http = FakeHttp()
    GitHubProvider(WebConfig(github_client_id="c"), client=http).exchange("code")
    assert not any("/orgs/" in url for _, url in http.calls)


def test_the_dev_provider_never_calls_github():
    identity, member = DevProvider("ana").exchange("anything")
    assert identity.login == "ana"
    assert member is True
