"""Which peers are allowed to speak for their clients through `X-Forwarded-For`.

Why this module exists (ClickUp 86cbbq6vz, incident 2026-08-30). The trusted
proxy used to be a single hardcoded container address in `TRUSTED_PROXY_IPS`.
Docker addresses are handed out in start order and do **not** survive a VM
reboot or resize: after the 2026-08-30 hard reboot the containers came up in a
different order, `172.18.0.5` became MySQL instead of nginx, and Bible-API
quietly stopped trusting the only proxy in front of it. Every client was then
recorded under nginx's own address (statistics collapsed into one row per
nginx) and `AI_REQUESTS_PER_CLIENT_PER_MINUTE` became a global limit, because
every caller hashed to the same pseudonym. Nothing was logged: the failure was
completely silent and was found by looking at suspicious statistics.

The fix has two halves:

1. **Trust a NAME, not an address.** `TRUSTED_PROXY_HOSTS=bible-web` is
   resolved through docker's embedded DNS at startup and re-resolved on a TTL,
   so the address the proxy holds *right now* is the address that is trusted.
   A reboot reshuffles the addresses and this keeps working with no `.env` edit.
2. **Make the failure loud.** The trust state is logged once at startup, every
   observed change of a proxy address is logged, a host that stops resolving is
   logged as an error, and an `X-Forwarded-For` arriving from a peer we do not
   trust is reported (once per peer per interval, never per request) — that
   header from an unexpected peer is the exact signature of the incident.

`TRUSTED_PROXY_IPS` is kept and extended with CIDR notation for deployments
that cannot use a name. See the honest trade-offs in the repository CLAUDE.md:
a whole-subnet entry (`172.18.0.0/16`) also survives reboots, but it trusts
every container on the docker network, so any one of them could forge
`X-Forwarded-For`; the name resolves to the proxy only.

Local development is unaffected: with nothing configured the module trusts
nothing, which is the correct behaviour for an API exposed directly.
"""

import logging
import socket
import threading
import time
from ipaddress import ip_address

from config import (
    TRUSTED_PROXY_DNS_TTL_SECONDS,
    TRUSTED_PROXY_HOSTS,
    TRUSTED_PROXY_IPS,
    TRUSTED_PROXY_NETWORKS,
)

logger = logging.getLogger(__name__)


def resolve_host_addresses(host: str) -> frozenset[str]:
    """Every address `host` currently resolves to. Raises OSError on failure."""
    infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    return frozenset(str(ip_address(info[4][0])) for info in infos)


class TrustedProxies:
    """The set of peers whose `X-Forwarded-For` is believed.

    Three kinds of entries, all optional:

    * `addresses` — exact peer strings (the parsed `TRUSTED_PROXY_IPS`);
    * `networks` — CIDR networks the peer address must fall into;
    * `hosts` — DNS names re-resolved every `ttl_seconds`.

    Resolution is **stale-while-revalidate**, and that is a correctness
    property, not a micro-optimisation: `is_trusted` runs inside the request
    path of an async application, and `socket.getaddrinfo` is a blocking call.
    A resolver that hangs (a name outside docker's DNS falling through to an
    unreachable upstream) stalls the *entire* event loop — every request, not
    just this one — for as long as the lookup takes, once per TTL.

    So the request path never resolves. It reads the current snapshot and
    returns; when that snapshot is older than the TTL it hands the work to a
    single background daemon thread, whose result atomically replaces the
    snapshot. Trust therefore follows a changed proxy address within one TTL
    plus one lookup, and a broken resolver costs nothing but staleness.

    The one synchronous resolution is the cold one: `log_startup_state()`
    primes the cache at startup (as before), because there is no previous
    snapshot to serve and answering "trust nobody" while a lookup is pending
    would be the very failure this module exists to prevent.
    """

    def __init__(
        self,
        addresses=(),
        networks=(),
        hosts=(),
        ttl_seconds: int = 30,
        resolver=resolve_host_addresses,
        clock=None,
    ) -> None:
        self._addresses = frozenset(addresses)
        self._networks = tuple(networks)
        self._hosts = tuple(hosts)
        self._ttl = max(1, int(ttl_seconds))
        self._resolver = resolver
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._resolved: dict[str, frozenset[str]] = {}
        self._failed: dict[str, str] = {}
        self._checked_at: float | None = None
        # The snapshot the request path reads: every address the configured
        # hosts resolved to at the last successful pass. Replaced whole, never
        # mutated, so a reader always sees one consistent generation.
        self._snapshot: frozenset[str] = frozenset()
        self._refreshing = False
        # Kept only so tests can join the background pass deterministically.
        self._refresh_thread: threading.Thread | None = None

    # -- configuration ------------------------------------------------------

    @property
    def configured(self) -> bool:
        """True when this deployment declares any proxy in front of the API."""
        return bool(self._addresses or self._networks or self._hosts)

    def describe(self) -> str:
        parts = []
        if self._addresses:
            parts.append("ips=" + ",".join(sorted(self._addresses)))
        if self._networks:
            parts.append("networks=" + ",".join(str(n) for n in self._networks))
        if self._hosts:
            # One read each: both dicts are rebound as whole objects by a
            # background refresh, so a second read could see a later
            # generation than the first.
            current, failures = self._resolved, self._failed
            resolved = ", ".join(
                f"{host}->"
                + (
                    ",".join(sorted(current[host]))
                    if current.get(host)
                    else f"UNRESOLVED ({failures.get(host, 'no addresses')})"
                )
                for host in self._hosts
            )
            parts.append(f"hosts=[{resolved}]")
        return "; ".join(parts) if parts else "nothing (API is exposed directly)"

    # -- resolution ---------------------------------------------------------

    def _lookup_all(self) -> dict[str, tuple[frozenset[str], str]]:
        """Resolve every configured host. Blocking, and never holds the lock.

        Holding `self._lock` across `getaddrinfo` would put the request path
        (which takes the same lock for a few microseconds) behind a call that
        can take seconds — exactly the stall this design avoids.
        """
        results: dict[str, tuple[frozenset[str], str]] = {}
        for host in self._hosts:
            try:
                current = self._resolver(host)
            except OSError as error:
                results[host] = (frozenset(), f"{type(error).__name__}: {error}")
            else:
                results[host] = (
                    current,
                    "" if current else "resolved to no addresses",
                )
        return results

    def _publish_locked(
        self, results: dict[str, tuple[frozenset[str], str]], reason: str
    ) -> None:
        """Install a lookup result as the new snapshot and narrate the change."""
        resolved = dict(self._resolved)
        failed = dict(self._failed)
        for host in self._hosts:
            previous = self._resolved.get(host)
            current, failure = results[host]

            if failure:
                # Loud, but only on the transition into failure: after that the
                # state is unchanged and repeating it every TTL is noise.
                if self._failed.get(host) != failure:
                    logger.error(
                        "Trusted proxy host %r does not resolve (%s) — "
                        "X-Forwarded-For from it will be IGNORED and clients "
                        "will be recorded as the proxy itself. Check "
                        "TRUSTED_PROXY_HOSTS and that the container is up "
                        "(%s)",
                        host,
                        failure,
                        reason,
                    )
                failed[host] = failure
                resolved[host] = frozenset()
                continue

            failed.pop(host, None)
            resolved[host] = current
            if previous is None:
                logger.info(
                    "Trusted proxy host %r resolves to %s (%s)",
                    host,
                    ",".join(sorted(current)),
                    reason,
                )
            elif previous != current:
                # The reboot case: the address moved and trust moved with it.
                logger.warning(
                    "Trusted proxy host %r changed address: %s -> %s (%s)",
                    host,
                    ",".join(sorted(previous)) or "-",
                    ",".join(sorted(current)),
                    reason,
                )
        # Rebound as whole objects, so a reader without the lock (describe())
        # always sees one consistent generation instead of a half-written dict.
        self._resolved = resolved
        self._failed = failed
        merged: set[str] = set()
        for addresses in resolved.values():
            merged.update(addresses)
        self._snapshot = frozenset(merged)
        self._checked_at = self._clock()

    def refresh(self, reason: str = "periodic refresh") -> None:
        """Re-resolve every configured host name now, blocking until done."""
        if not self._hosts:
            with self._lock:
                self._checked_at = self._clock()
                self._refreshing = False
            return
        try:
            results = self._lookup_all()
        except BaseException:
            # A resolver failing in some way other than OSError must not leave
            # the "a refresh is in flight" flag stuck: that would freeze the
            # snapshot forever, and silently, which is the failure mode this
            # whole module exists to rule out.
            with self._lock:
                self._refreshing = False
            raise
        with self._lock:
            try:
                self._publish_locked(results, reason)
            finally:
                self._refreshing = False

    def _refresh_in_background(self, reason: str) -> None:
        try:
            self.refresh(reason)
        except Exception:  # pragma: no cover - a resolver must never kill this
            logger.exception("Trusted proxy refresh failed unexpectedly")
            with self._lock:
                self._refreshing = False

    def _host_addresses(self) -> frozenset[str]:
        """The snapshot, without ever blocking on DNS in the request path."""
        if not self._hosts:
            return frozenset()

        with self._lock:
            cold = self._checked_at is None
            stale = cold or self._clock() - self._checked_at >= self._ttl
            start = stale and not self._refreshing
            if start:
                self._refreshing = True
            snapshot = self._snapshot

        if not start:
            return snapshot
        if cold:
            # Nothing to serve yet. In a real process this never happens —
            # main.py primes the cache through log_startup_state() before the
            # first request — so this is the "constructed and used directly"
            # path, where blocking once is right and costs one lookup.
            self.refresh("first use")
            with self._lock:
                return self._snapshot

        # Stale but usable: answer from the old snapshot immediately and let a
        # single background thread install the new one. At most one is in
        # flight (_refreshing), so a slow resolver cannot pile threads up.
        thread = threading.Thread(
            target=self._refresh_in_background,
            args=("TTL expired",),
            name="trusted-proxy-dns",
            daemon=True,
        )
        self._refresh_thread = thread
        thread.start()
        return snapshot

    # -- the question this module answers -----------------------------------

    def is_trusted(self, peer: str) -> bool:
        if not self.configured:
            return False
        if peer in self._addresses:
            return True

        try:
            parsed = ip_address(peer)
        except ValueError:
            parsed = None

        # A peer arrives as a string, a configured address as a parsed one, and
        # the same address has several spellings: an abbreviated IPv6, or the
        # IPv4-mapped form (`::ffff:172.18.0.4`) a dual-stack listener reports
        # for an IPv4 connection. Comparing spellings instead of addresses is
        # exactly how this check fails silently, so compare every form.
        forms = {peer}
        if parsed is not None:
            forms.add(str(parsed))
            mapped = getattr(parsed, "ipv4_mapped", None)
            if mapped is not None:
                forms.add(str(mapped))
                parsed = mapped

        if forms & self._addresses:
            return True
        if self._hosts and forms & self._host_addresses():
            return True
        if parsed is not None and any(parsed in net for net in self._networks):
            return True
        return False

    @staticmethod
    def _ensure_visible_handler() -> None:
        """Make sure the startup banner actually reaches `docker logs`.

        Uvicorn configures its own loggers and leaves the root logger without
        handlers, so an INFO record from an application module is swallowed:
        only WARNING and above reach stderr, through logging's last-resort
        handler. The whole point of this ticket is that the proxy
        configuration must not be invisible, so this one logger gets a handler
        of its own when nothing else has installed one (under pytest, or with
        any real logging configuration, this is a no-op).
        """
        if logger.handlers or logging.getLogger().handlers:
            return
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s:     %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    def log_startup_state(self) -> None:
        """One loud line at startup saying what this process trusts, and why.

        Called from `main.py`. A deployment behind a proxy that trusts nothing
        is the silent failure of 2026-08-30, so it is an error, not an info
        line — but it is never fatal: refusing to start would take the whole
        public API down over statistics accuracy, and the API in front of a
        proxy that is merely slow to come up would never recover.
        """
        self._ensure_visible_handler()
        self.refresh("startup")
        if not self.configured:
            logger.info(
                "Trusted proxies: none configured — X-Forwarded-For is "
                "ignored and the peer address is the client. Correct only "
                "when the API is exposed directly (no nginx in front); "
                "otherwise set TRUSTED_PROXY_HOSTS."
            )
            return
        if self._hosts and not any(self._resolved.values()) and not (
            self._addresses or self._networks
        ):
            logger.error(
                "Trusted proxies: NONE of the configured hosts resolve (%s) — "
                "every client will be recorded as the proxy address and the "
                "per-client rate limit will act as a global one",
                self.describe(),
            )
            return
        logger.info("Trusted proxies: %s", self.describe())


TRUSTED_PROXIES = TrustedProxies(
    addresses=TRUSTED_PROXY_IPS,
    networks=TRUSTED_PROXY_NETWORKS,
    hosts=TRUSTED_PROXY_HOSTS,
    ttl_seconds=TRUSTED_PROXY_DNS_TTL_SECONDS,
)
