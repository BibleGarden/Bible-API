"""Trusted-proxy resolution and the loud diagnostics around it.

Regression suite for ClickUp 86cbbq6vz: on 2026-08-30 a hard reboot of the
production VM reshuffled the docker addresses, `TRUSTED_PROXY_IPS=172.18.0.5`
started pointing at MySQL instead of nginx, and Bible-API silently stopped
believing `X-Forwarded-For`. Statistics recorded every client as the nginx
container and the per-client AI rate limit became a global one. Nothing was
logged.

Two properties are pinned here: the trust must follow the proxy when its
address changes (name resolution + TTL), and every failure mode must produce a
log line — at startup, on a resolution failure, and on the first forwarded
header from a peer that is not trusted.
"""

import logging
import os
import threading
import time

import pytest

os.environ.setdefault("API_KEY", "test-api-key")

import client_ip  # noqa: E402
import config  # noqa: E402
from trusted_proxies import TrustedProxies  # noqa: E402


NGINX = "172.18.0.4"
MYSQL = "172.18.0.3"


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class FakeResolver:
    """Docker's embedded DNS, as far as these tests are concerned."""

    def __init__(self, table):
        self.table = dict(table)
        self.calls = 0
        self.entered = threading.Event()
        # When set, the "lookup" hangs until the test releases it — the hung
        # resolver whose blocking getaddrinfo used to stall the event loop.
        self.gate: threading.Event | None = None

    def __call__(self, host):
        self.calls += 1
        self.entered.set()
        if self.gate is not None:
            assert self.gate.wait(5), "test forgot to release the resolver"
        try:
            value = self.table[host]
        except KeyError:
            raise OSError(f"[Errno -2] Name or service not known: {host}") from None
        if isinstance(value, Exception):
            raise value
        return frozenset(value)


def settle(proxies, timeout=5.0):
    """Wait for the background re-resolution `is_trusted` may have started.

    Resolution is stale-while-revalidate now (M2): the request path returns the
    previous snapshot immediately and a daemon thread installs the new one, so
    a test that asserts on the *new* state has to wait for that thread.
    """
    thread = proxies._refresh_thread
    if thread is not None:
        thread.join(timeout)
        assert not thread.is_alive(), "background resolution did not finish"


# ---------------------------------------------------------------------------
# Parsing: TRUSTED_PROXY_IPS (addresses and CIDR networks)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", [None, "", "   ", ",", " , "])
def test_empty_ips_mean_no_proxy(raw):
    # A supported deployment (the local machine), not a missing setting.
    assert config.parse_trusted_proxy_ips("TRUSTED_PROXY_IPS", raw) == (
        frozenset(), (),
    )


def test_ips_are_parsed_and_whitespace_ignored():
    addresses, networks = config.parse_trusted_proxy_ips(
        "TRUSTED_PROXY_IPS", " 172.18.0.4 , 127.0.0.1"
    )
    assert addresses == {"172.18.0.4", "127.0.0.1"}
    assert networks == ()


def test_cidr_networks_are_parsed():
    addresses, networks = config.parse_trusted_proxy_ips(
        "TRUSTED_PROXY_IPS", "172.18.0.0/16"
    )
    assert addresses == frozenset()
    assert [str(net) for net in networks] == ["172.18.0.0/16"]


def test_ipv6_addresses_are_normalized():
    addresses, _ = config.parse_trusted_proxy_ips(
        "TRUSTED_PROXY_IPS", "2001:0db8:0000::1"
    )
    assert addresses == {"2001:db8::1"}


@pytest.mark.parametrize(
    "raw", ["bible-web", "172.18.0.4;", "172.18.0", "172.18.0.4 172.18.0.5"]
)
def test_garbage_ips_abort_startup_naming_the_variable(raw):
    with pytest.raises(config.ConfigError) as exc:
        config.parse_trusted_proxy_ips("TRUSTED_PROXY_IPS", raw)
    assert "TRUSTED_PROXY_IPS" in str(exc.value)
    assert raw in str(exc.value)


def test_a_network_with_host_bits_set_is_rejected():
    # "one address" and "the whole subnet" are very different amounts of
    # trust; a strict parse is what tells them apart.
    with pytest.raises(config.ConfigError) as exc:
        config.parse_trusted_proxy_ips("TRUSTED_PROXY_IPS", "172.18.0.5/16")
    assert "TRUSTED_PROXY_IPS" in str(exc.value)
    assert "172.18.0.5/16" in str(exc.value)


def test_an_ip_entry_points_at_the_hosts_variable():
    with pytest.raises(config.ConfigError) as exc:
        config.parse_trusted_proxy_ips("TRUSTED_PROXY_IPS", "bible-web")
    assert "TRUSTED_PROXY_HOSTS" in str(exc.value)


# ---------------------------------------------------------------------------
# Parsing: TRUSTED_PROXY_HOSTS
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", [None, "", "  ", ","])
def test_empty_hosts_mean_no_names(raw):
    assert config.parse_trusted_proxy_hosts("TRUSTED_PROXY_HOSTS", raw) == ()


def test_hosts_keep_order_and_are_deduplicated():
    assert config.parse_trusted_proxy_hosts(
        "TRUSTED_PROXY_HOSTS", " bible-web , edge.example.com, bible-web "
    ) == ("bible-web", "edge.example.com")


@pytest.mark.parametrize("raw", ["bible web", "-bible-web", "bible_web", "a/b"])
def test_garbage_hosts_abort_startup_naming_the_variable(raw):
    with pytest.raises(config.ConfigError) as exc:
        config.parse_trusted_proxy_hosts("TRUSTED_PROXY_HOSTS", raw)
    assert "TRUSTED_PROXY_HOSTS" in str(exc.value)
    assert raw in str(exc.value)


def test_an_address_in_the_hosts_variable_is_an_error():
    with pytest.raises(config.ConfigError) as exc:
        config.parse_trusted_proxy_hosts("TRUSTED_PROXY_HOSTS", "172.18.0.4")
    assert "TRUSTED_PROXY_IPS" in str(exc.value)


def test_both_variables_report_through_the_aggregated_config_error():
    # The parse errors must join ADR 0008's single "here is everything that is
    # wrong" message, not abort on the first one — that is how config.py
    # collects them (see _get_trusted_ips / _get_trusted_hosts).
    problems = []
    for parse, raw, name in (
        (config.parse_trusted_proxy_ips, "nope", "TRUSTED_PROXY_IPS"),
        (config.parse_trusted_proxy_hosts, "no host", "TRUSTED_PROXY_HOSTS"),
    ):
        with pytest.raises(config.ConfigError) as parse_error:
            parse(name, raw)
        problems.append(str(parse_error.value))

    with pytest.raises(config.ConfigError) as exc:
        config._validate(
            {
                "API_KEY": "k",
                "DB_HOST": "h",
                "DB_USER": "u",
                "DB_PASSWORD": "p",
                "DB_NAME": "n",
                "EMBEDDING_MODEL": "m",
                "EMBEDDING_DIMENSIONS": "768",
            },
            problems,
        )
    message = str(exc.value)
    assert "TRUSTED_PROXY_IPS" in message
    assert "TRUSTED_PROXY_HOSTS" in message
    assert "2 problems" in message


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def test_nothing_configured_trusts_nobody():
    proxies = TrustedProxies()
    assert not proxies.configured
    assert not proxies.is_trusted("127.0.0.1")
    assert not proxies.is_trusted(NGINX)
    assert "nothing" in proxies.describe()


def test_literal_address_is_trusted():
    proxies = TrustedProxies(addresses={NGINX})
    assert proxies.is_trusted(NGINX)
    assert not proxies.is_trusted(MYSQL)


def test_cidr_network_trusts_the_whole_subnet():
    _, networks = config.parse_trusted_proxy_ips(
        "TRUSTED_PROXY_IPS", "172.18.0.0/16"
    )
    proxies = TrustedProxies(networks=networks)
    assert proxies.is_trusted(NGINX)
    assert proxies.is_trusted(MYSQL)  # honestly: every container, not just nginx
    assert not proxies.is_trusted("203.0.113.7")
    assert not proxies.is_trusted("not-an-ip")


def test_host_name_is_resolved_to_the_current_address():
    resolver = FakeResolver({"bible-web": {NGINX}})
    proxies = TrustedProxies(hosts=("bible-web",), resolver=resolver)
    assert proxies.is_trusted(NGINX)
    assert not proxies.is_trusted(MYSQL)


def test_the_incident_trust_follows_the_proxy_across_a_reboot():
    # 2026-08-30: the containers came back in a different order, nginx moved
    # from .5 to .4 and .5 became MySQL. With a name, no .env edit is needed.
    clock = FakeClock()
    resolver = FakeResolver({"bible-web": {"172.18.0.5"}})
    proxies = TrustedProxies(
        hosts=("bible-web",), ttl_seconds=30, resolver=resolver, clock=clock
    )
    assert proxies.is_trusted("172.18.0.5")

    resolver.table["bible-web"] = {NGINX}  # the reboot
    clock.advance(31)
    proxies.is_trusted(MYSQL)  # the request that notices the TTL expired
    settle(proxies)
    assert proxies.is_trusted(NGINX)
    assert not proxies.is_trusted("172.18.0.5")


def test_resolution_happens_at_most_once_per_ttl():
    clock = FakeClock()
    resolver = FakeResolver({"bible-web": {NGINX}})
    proxies = TrustedProxies(
        hosts=("bible-web",), ttl_seconds=30, resolver=resolver, clock=clock
    )
    for _ in range(10):
        proxies.is_trusted(MYSQL)
    settle(proxies)
    assert resolver.calls == 1
    clock.advance(29)
    proxies.is_trusted(MYSQL)
    settle(proxies)
    assert resolver.calls == 1
    clock.advance(2)
    proxies.is_trusted(MYSQL)
    settle(proxies)
    assert resolver.calls == 2


def test_the_request_path_never_waits_for_dns():
    """M2: an expired TTL must not park the event loop on getaddrinfo.

    `is_trusted` runs in the request path of an async application and the
    resolver call is blocking, so a name that falls through docker's DNS to an
    unreachable upstream used to stall *every* request for the length of the
    lookup, once per TTL. Now the expired snapshot is served as-is and the
    lookup happens on a background thread.
    """
    clock = FakeClock()
    resolver = FakeResolver({"bible-web": {"172.18.0.5"}})
    proxies = TrustedProxies(
        hosts=("bible-web",), ttl_seconds=30, resolver=resolver, clock=clock
    )
    assert proxies.is_trusted("172.18.0.5")  # cold: primed synchronously

    gate = threading.Event()
    resolver.gate = gate
    resolver.entered.clear()
    resolver.table["bible-web"] = {NGINX}
    clock.advance(31)

    started = time.monotonic()
    for _ in range(20):
        # The hot path answers from the stale snapshot while the resolver hangs.
        assert proxies.is_trusted("172.18.0.5")
        assert not proxies.is_trusted(NGINX)
    assert time.monotonic() - started < 1.0
    assert resolver.entered.wait(5)  # ...and a refresh really is under way
    assert resolver.calls == 2  # exactly one, not one per request

    gate.set()
    settle(proxies)
    assert proxies.is_trusted(NGINX)
    assert not proxies.is_trusted("172.18.0.5")


def test_a_trusted_literal_short_circuits_before_dns():
    resolver = FakeResolver({"bible-web": {NGINX}})
    proxies = TrustedProxies(
        addresses={"127.0.0.1"}, hosts=("bible-web",), resolver=resolver
    )
    assert proxies.is_trusted("127.0.0.1")
    assert resolver.calls == 0


@pytest.mark.parametrize("peer", ["172.18.0.4", "::ffff:172.18.0.4"])
def test_an_ipv4_mapped_peer_is_the_same_address(peer):
    # A dual-stack listener reports an IPv4 connection in the mapped form;
    # comparing spellings instead of addresses is how this check would fail
    # silently again.
    assert TrustedProxies(addresses={NGINX}).is_trusted(peer)
    _, networks = config.parse_trusted_proxy_ips(
        "TRUSTED_PROXY_IPS", "172.18.0.0/16"
    )
    assert TrustedProxies(networks=networks).is_trusted(peer)
    assert TrustedProxies(
        hosts=("bible-web",), resolver=FakeResolver({"bible-web": {NGINX}})
    ).is_trusted(peer)


def test_the_sources_are_additive():
    resolver = FakeResolver({"bible-web": {NGINX}})
    proxies = TrustedProxies(
        addresses={"127.0.0.1"}, hosts=("bible-web",), resolver=resolver
    )
    assert proxies.is_trusted("127.0.0.1")
    assert proxies.is_trusted(NGINX)
    assert not proxies.is_trusted(MYSQL)


# ---------------------------------------------------------------------------
# Loud diagnostics
# ---------------------------------------------------------------------------


def test_an_unresolvable_host_is_logged_as_an_error_once(caplog):
    clock = FakeClock()
    resolver = FakeResolver({})
    proxies = TrustedProxies(
        hosts=("bible-web",), ttl_seconds=30, resolver=resolver, clock=clock
    )
    with caplog.at_level(logging.ERROR, logger="trusted_proxies"):
        proxies.is_trusted(MYSQL)
        settle(proxies)
        clock.advance(31)
        proxies.is_trusted(MYSQL)  # second failed resolution, same state
        settle(proxies)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "bible-web" in errors[0].getMessage()
    assert "TRUSTED_PROXY_HOSTS" in errors[0].getMessage()
    assert resolver.calls == 2  # it kept retrying, it just stopped shouting


def test_a_changed_proxy_address_is_logged(caplog):
    clock = FakeClock()
    resolver = FakeResolver({"bible-web": {"172.18.0.5"}})
    proxies = TrustedProxies(
        hosts=("bible-web",), ttl_seconds=30, resolver=resolver, clock=clock
    )
    proxies.is_trusted(MYSQL)
    settle(proxies)
    with caplog.at_level(logging.WARNING, logger="trusted_proxies"):
        resolver.table["bible-web"] = {NGINX}
        clock.advance(31)
        proxies.is_trusted(MYSQL)
        settle(proxies)
    message = "\n".join(r.getMessage() for r in caplog.records)
    assert "172.18.0.5" in message and NGINX in message


def test_recovery_after_a_failure_is_logged_too(caplog):
    clock = FakeClock()
    resolver = FakeResolver({})
    proxies = TrustedProxies(
        hosts=("bible-web",), ttl_seconds=30, resolver=resolver, clock=clock
    )
    proxies.is_trusted(MYSQL)
    settle(proxies)
    with caplog.at_level(logging.INFO, logger="trusted_proxies"):
        resolver.table["bible-web"] = {NGINX}
        clock.advance(31)
        proxies.is_trusted(MYSQL)
        settle(proxies)
        assert proxies.is_trusted(NGINX)
    assert any(NGINX in r.getMessage() for r in caplog.records)


def test_startup_logs_what_is_trusted(caplog):
    resolver = FakeResolver({"bible-web": {NGINX}})
    proxies = TrustedProxies(hosts=("bible-web",), resolver=resolver)
    with caplog.at_level(logging.INFO, logger="trusted_proxies"):
        proxies.log_startup_state()
    message = "\n".join(r.getMessage() for r in caplog.records)
    assert "bible-web" in message and NGINX in message


def test_startup_without_any_proxy_says_so(caplog):
    with caplog.at_level(logging.INFO, logger="trusted_proxies"):
        TrustedProxies().log_startup_state()
    message = "\n".join(r.getMessage() for r in caplog.records)
    assert "none configured" in message
    # ...and it is not an error: running without a proxy is supported.
    assert all(r.levelno < logging.ERROR for r in caplog.records)


def test_startup_behind_an_unresolvable_proxy_is_an_error(caplog):
    proxies = TrustedProxies(hosts=("bible-web",), resolver=FakeResolver({}))
    with caplog.at_level(logging.ERROR, logger="trusted_proxies"):
        proxies.log_startup_state()
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors
    assert "bible-web" in "\n".join(r.getMessage() for r in errors)


def test_startup_never_raises_on_a_broken_resolver():
    # A proxy that is slow to come up must not take the public API down.
    proxies = TrustedProxies(hosts=("bible-web",), resolver=FakeResolver({}))
    proxies.log_startup_state()
    assert proxies.configured


# ---------------------------------------------------------------------------
# resolve_client_ip
# ---------------------------------------------------------------------------


class FakeRequest:
    def __init__(self, peer, headers=None):
        self.client = None if peer is None else type("C", (), {"host": peer})()
        self.headers = headers or {}


@pytest.fixture(autouse=True)
def _reset_reporting():
    client_ip._reset_report_state()
    yield
    client_ip._reset_report_state()


def test_forwarded_header_from_an_untrusted_peer_is_ignored(monkeypatch):
    # The spoofing case: without a trusted proxy the client IS the peer.
    monkeypatch.setattr(client_ip, "TRUSTED_PROXIES", TrustedProxies())
    request = FakeRequest("203.0.113.9", {"x-forwarded-for": "10.0.0.1"})
    assert client_ip.resolve_client_ip(request) == "203.0.113.9"


def test_forwarded_header_from_a_trusted_peer_is_used(monkeypatch):
    monkeypatch.setattr(
        client_ip,
        "TRUSTED_PROXIES",
        TrustedProxies(hosts=("bible-web",), resolver=FakeResolver({"bible-web": {NGINX}})),
    )
    # RIGHTMOST, not leftmost: nginx appends the address it saw to whatever the
    # client sent, so the last element is the one our own proxy vouched for.
    request = FakeRequest(NGINX, {"x-forwarded-for": "203.0.113.7, 192.0.2.1"})
    assert client_ip.resolve_client_ip(request) == "192.0.2.1"


def test_a_forged_left_element_is_ignored(monkeypatch):
    """M1, the spoof this ticket closes.

    Production nginx uses `$proxy_add_x_forwarded_for` in every location: it
    KEEPS the header the client sent and appends `$remote_addr`. Reading the
    leftmost element therefore let any caller declare its own address —
    poisoning `api_requests` and, with a fresh value per request, evading
    `AI_REQUESTS_PER_CLIENT_PER_MINUTE`, whose bucket is keyed by it.
    """
    monkeypatch.setattr(
        client_ip, "TRUSTED_PROXIES", TrustedProxies(addresses={NGINX})
    )
    for forgery in ("1.2.3.4", "8.8.8.8, 9.9.9.9", NGINX, "203.0.113.77"):
        request = FakeRequest(
            NGINX, {"x-forwarded-for": f"{forgery}, 192.0.2.1"}
        )
        assert client_ip.resolve_client_ip(request) == "192.0.2.1"


def test_a_single_element_header_is_the_client(monkeypatch):
    # The honest request: the client sent no X-Forwarded-For, nginx created it.
    monkeypatch.setattr(
        client_ip, "TRUSTED_PROXIES", TrustedProxies(addresses={NGINX})
    )
    request = FakeRequest(NGINX, {"x-forwarded-for": "192.0.2.1"})
    assert client_ip.resolve_client_ip(request) == "192.0.2.1"


def test_trusted_hops_are_skipped_right_to_left(monkeypatch):
    # Not today's topology (we have exactly one hop), but the reason the walk
    # is written as a walk: a second trusted proxy must not become "the client".
    monkeypatch.setattr(
        client_ip,
        "TRUSTED_PROXIES",
        TrustedProxies(addresses={NGINX, "172.18.0.9"}),
    )
    request = FakeRequest(
        NGINX, {"x-forwarded-for": "1.2.3.4, 192.0.2.1, 172.18.0.9"}
    )
    assert client_ip.resolve_client_ip(request) == "192.0.2.1"


def test_a_chain_of_nothing_but_trusted_proxies_falls_back_inward(monkeypatch):
    monkeypatch.setattr(
        client_ip,
        "TRUSTED_PROXIES",
        TrustedProxies(addresses={NGINX, "172.18.0.9"}),
    )
    request = FakeRequest(NGINX, {"x-forwarded-for": "172.18.0.9"})
    assert client_ip.resolve_client_ip(request) == "172.18.0.9"


@pytest.mark.parametrize(
    "header", ["not-an-ip", "192.0.2.1, ", "192.0.2.1,not-an-ip", "  ", ","]
)
def test_a_malformed_forwarded_header_falls_back_to_the_peer(monkeypatch, header):
    # nginx never produces one of these, so a header that cannot be read as a
    # hop chain is not read at all: the direct peer is the client.
    monkeypatch.setattr(
        client_ip, "TRUSTED_PROXIES", TrustedProxies(addresses={NGINX})
    )
    request = FakeRequest(NGINX, {"x-forwarded-for": header})
    assert client_ip.resolve_client_ip(request) == NGINX


def test_no_header_no_noise(monkeypatch, caplog):
    monkeypatch.setattr(
        client_ip, "TRUSTED_PROXIES", TrustedProxies(addresses={NGINX})
    )
    with caplog.at_level(logging.WARNING, logger="client_ip"):
        assert client_ip.resolve_client_ip(FakeRequest("203.0.113.9")) == "203.0.113.9"
    assert caplog.records == []


def test_untrusted_forwarding_is_reported_once_not_per_request(monkeypatch, caplog):
    # The incident's signature: nginx is in front of us, forwards headers, and
    # we do not trust it. One error, not one per request.
    monkeypatch.setattr(
        client_ip, "TRUSTED_PROXIES", TrustedProxies(addresses={"172.18.0.5"})
    )
    request = FakeRequest(NGINX, {"x-forwarded-for": "203.0.113.7"})
    with caplog.at_level(logging.ERROR, logger="client_ip"):
        for _ in range(50):
            assert client_ip.resolve_client_ip(request) == NGINX
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    message = errors[0].getMessage()
    assert NGINX in message
    assert "TRUSTED_PROXY_HOSTS" in message


def test_each_untrusted_peer_is_reported_separately(monkeypatch, caplog):
    monkeypatch.setattr(
        client_ip, "TRUSTED_PROXIES", TrustedProxies(addresses={"172.18.0.5"})
    )
    with caplog.at_level(logging.ERROR, logger="client_ip"):
        client_ip.resolve_client_ip(FakeRequest(NGINX, {"x-forwarded-for": "1.1.1.1"}))
        client_ip.resolve_client_ip(FakeRequest(MYSQL, {"x-forwarded-for": "1.1.1.1"}))
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 2


def test_the_report_repeats_after_the_interval(monkeypatch, caplog):
    monkeypatch.setattr(
        client_ip, "TRUSTED_PROXIES", TrustedProxies(addresses={"172.18.0.5"})
    )
    request = FakeRequest(NGINX, {"x-forwarded-for": "203.0.113.7"})
    with caplog.at_level(logging.ERROR, logger="client_ip"):
        client_ip.resolve_client_ip(request)
        client_ip._reported_peers[NGINX] -= (
            client_ip.FORWARDING_LOG_INTERVAL_SECONDS + 1
        )
        client_ip.resolve_client_ip(request)
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 2


def test_without_a_configured_proxy_the_report_is_a_warning(monkeypatch, caplog):
    # Local development: an API exposed directly. Worth a line (a forwarded
    # header from nowhere is still information), but not an error.
    monkeypatch.setattr(client_ip, "TRUSTED_PROXIES", TrustedProxies())
    with caplog.at_level(logging.DEBUG, logger="client_ip"):
        client_ip.resolve_client_ip(
            FakeRequest("203.0.113.9", {"x-forwarded-for": "10.0.0.1"})
        )
    assert [r.levelno for r in caplog.records] == [logging.WARNING]
    assert "TRUSTED_PROXY_HOSTS" in caplog.records[0].getMessage()


def test_the_reported_peer_table_is_bounded(monkeypatch):
    monkeypatch.setattr(client_ip, "TRUSTED_PROXIES", TrustedProxies())
    monkeypatch.setattr(client_ip, "_MAX_REPORTS_PER_INTERVAL", 10_000)
    for octet in range(client_ip._MAX_TRACKED_PEERS + 5):
        client_ip.resolve_client_ip(
            FakeRequest(f"10.0.0.{octet}", {"x-forwarded-for": "1.1.1.1"})
        )
    assert len(client_ip._reported_peers) <= client_ip._MAX_TRACKED_PEERS


def test_a_full_table_evicts_the_oldest_peer_not_all_of_them(monkeypatch):
    """m1: overflow must not hand the attacker an amnesia button.

    The table used to be `clear()`ed when full, so a caller cycling through
    addresses could erase the record of the genuinely misconfigured proxy and
    have it re-reported on its very next request — one log line per request
    again, chosen by the attacker. Evicting the oldest entry keeps the recent
    ones (including the one that matters) suppressed.
    """
    monkeypatch.setattr(client_ip, "TRUSTED_PROXIES", TrustedProxies())
    monkeypatch.setattr(client_ip, "_MAX_REPORTS_PER_INTERVAL", 10_000)
    victim = "10.9.9.9"
    client_ip.resolve_client_ip(FakeRequest(victim, {"x-forwarded-for": "1.1.1.1"}))
    for octet in range(client_ip._MAX_TRACKED_PEERS + 5):
        client_ip.resolve_client_ip(
            FakeRequest(f"10.0.0.{octet}", {"x-forwarded-for": "1.1.1.1"})
        )
    # The first peer is the oldest, so it is the one that goes...
    assert victim not in client_ip._reported_peers
    # ...and the flood did not wipe the rest of the table with it.
    assert len(client_ip._reported_peers) == client_ip._MAX_TRACKED_PEERS


def test_the_untrusted_report_has_a_global_ceiling(monkeypatch, caplog):
    """m1: the volume of this log must not be chosen by the caller.

    The trigger is a request header from an arbitrary peer, so per-peer
    throttling alone still lets a caller with many source addresses write one
    line per address. N lines per interval, whoever is asking.
    """
    monkeypatch.setattr(client_ip, "TRUSTED_PROXIES", TrustedProxies())
    with caplog.at_level(logging.WARNING, logger="client_ip"):
        for octet in range(client_ip._MAX_REPORTS_PER_INTERVAL * 5):
            client_ip.resolve_client_ip(
                FakeRequest(f"10.0.0.{octet}", {"x-forwarded-for": "1.1.1.1"})
            )
    assert len(caplog.records) == client_ip._MAX_REPORTS_PER_INTERVAL


def test_the_ceiling_lifts_with_the_next_interval(monkeypatch, caplog):
    # Silenced peers are not remembered as "already reported", so the real
    # misconfiguration is reported again as soon as there is budget for it.
    monkeypatch.setattr(client_ip, "TRUSTED_PROXIES", TrustedProxies())
    for octet in range(client_ip._MAX_REPORTS_PER_INTERVAL + 1):
        client_ip.resolve_client_ip(
            FakeRequest(f"10.0.0.{octet}", {"x-forwarded-for": "1.1.1.1"})
        )
    silenced = FakeRequest("10.0.0.250", {"x-forwarded-for": "1.1.1.1"})
    with caplog.at_level(logging.WARNING, logger="client_ip"):
        caplog.clear()  # the ten lines the budget did allow, above
        client_ip.resolve_client_ip(silenced)
        assert caplog.records == []
        client_ip._report_window_started -= (
            client_ip.FORWARDING_LOG_INTERVAL_SECONDS + 1
        )
        client_ip.resolve_client_ip(silenced)
    assert len(caplog.records) == 1


def test_a_missing_peer_is_never_trusted(monkeypatch):
    monkeypatch.setattr(
        client_ip, "TRUSTED_PROXIES", TrustedProxies(addresses={NGINX})
    )
    request = FakeRequest(None, {"x-forwarded-for": "203.0.113.7"})
    assert client_ip.resolve_client_ip(request) == "unknown"
