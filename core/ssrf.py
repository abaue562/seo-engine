"""SSRF Validator -- blocks outbound HTTP to private/metadata IP ranges."""
from __future__ import annotations
import ipaddress, logging, socket
from urllib.parse import urlparse

log = logging.getLogger(__name__)

_BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]
_BLOCKED_SCHEMES = frozenset({"file", "ftp", "gopher", "javascript", "data", "ldap", "dict"})


class SSRFError(ValueError):
    """Raised when a URL resolves to a blocked network range."""


def _is_blocked_ip(ip_str: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip_str)
        return any(addr in net for net in _BLOCKED_NETWORKS)
    except ValueError:
        return False


def validate_url(url: str, *, resolve_dns: bool = True) -> None:
    """Validate that *url* is safe to fetch. Raises SSRFError if blocked."""
    if not url or not isinstance(url, str):
        raise SSRFError("URL must be a non-empty string")
    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme in _BLOCKED_SCHEMES:
        raise SSRFError(f"Blocked scheme: {scheme!r}")
    if scheme not in ("http", "https", ""):
        if scheme:
            raise SSRFError(f"Unrecognised scheme: {scheme!r}")
    host = parsed.hostname or ""
    if not host:
        raise SSRFError("URL has no host")
    _loopback_names = {"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"}
    if host.lower() in _loopback_names:
        raise SSRFError(f"Blocked hostname: {host!r} (loopback alias)")
    # IP literal check -- re-raise SSRFError before except ValueError catches it
    # (SSRFError is a subclass of ValueError)
    try:
        addr = ipaddress.ip_address(host)
        if _is_blocked_ip(str(addr)):
            raise SSRFError(f"Blocked IP literal: {host!r}")
    except SSRFError:
        raise
    except ValueError:
        pass  # Not an IP literal, proceed to DNS
    if resolve_dns:
        try:
            infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
            for _f, _t, _p, _c, sockaddr in infos:
                ip = sockaddr[0]
                if _is_blocked_ip(ip):
                    log.warning("ssrf.blocked  host=%s  resolved_ip=%s", host, ip)
                    raise SSRFError(f"Host {host!r} resolves to blocked IP {ip!r}")
        except SSRFError:
            raise
        except OSError as e:
            log.debug("ssrf.dns_fail  host=%s  err=%s", host, e)
    log.debug("ssrf.ok  url=%s", url[:120])


def safe_httpx_client():
    """Return httpx.AsyncClient with SSRF validation wired to request hooks."""
    import httpx
    def _on_request(request: httpx.Request) -> None:
        validate_url(str(request.url), resolve_dns=False)
    return httpx.AsyncClient(
        event_hooks={"request": [_on_request]},
        timeout=30,
        follow_redirects=False,
    )


def wrap_requests_session():
    """Return a requests.Session that validates every URL before sending."""
    import requests
    class _SSRFSession(requests.Session):
        def request(self, method, url, **kwargs):
            validate_url(url)
            return super().request(method, url, **kwargs)
    return _SSRFSession()
