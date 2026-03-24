"""
data/fetchers/utils.py

Shared utilities for all ACA and AEMET fetcher modules.
Provides: retry logic, rate limiting, logging setup, common HTTP helpers.
"""
import logging
import time
import functools
from datetime import datetime, timezone
from typing import Callable, Any, Optional

import httpx


logger = logging.getLogger(__name__)


# ── Retry decorator ────────────────────────────────────────────────────────────

def with_retry(
    max_attempts: int = 3,
    backoff_base: float = 2.0,
    retryable_status_codes: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> Callable:
    """
    Decorator that retries a function on httpx errors or retryable HTTP status codes.

    Uses exponential backoff: wait = backoff_base ** attempt seconds.
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code in retryable_status_codes:
                        wait = backoff_base ** attempt
                        logger.warning(
                            f"{fn.__name__}: HTTP {e.response.status_code} on attempt "
                            f"{attempt}/{max_attempts}. Retrying in {wait:.1f}s."
                        )
                        time.sleep(wait)
                        last_exc = e
                    else:
                        raise
                except (httpx.ConnectError, httpx.TimeoutException) as e:
                    wait = backoff_base ** attempt
                    logger.warning(
                        f"{fn.__name__}: Network error on attempt {attempt}/{max_attempts}: "
                        f"{type(e).__name__}. Retrying in {wait:.1f}s."
                    )
                    time.sleep(wait)
                    last_exc = e
            logger.error(f"{fn.__name__}: All {max_attempts} attempts failed.")
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


# ── Rate limiter ───────────────────────────────────────────────────────────────

class RateLimiter:
    """
    Simple token-bucket rate limiter.

    Usage:
        limiter = RateLimiter(calls_per_minute=50)
        limiter.wait()  # blocks until the next call is allowed
    """

    def __init__(self, calls_per_minute: int = 50) -> None:
        self._min_interval: float = 60.0 / calls_per_minute
        self._last_call: float = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        remaining = self._min_interval - elapsed
        if remaining > 0:
            logger.debug(f"RateLimiter: sleeping {remaining:.2f}s")
            time.sleep(remaining)
        self._last_call = time.monotonic()


# ── Shared HTTP client factory ─────────────────────────────────────────────────

def make_client(timeout: float = 60.0) -> httpx.Client:
    """Return a configured httpx.Client with sensible defaults."""
    return httpx.Client(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": "llobregat-twin/1.0 (watershed-monitoring)"},
    )


# ── Timestamp helpers ──────────────────────────────────────────────────────────

def utc_now() -> datetime:
    """Return current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


def format_aemet_datetime(dt: datetime) -> str:
    """
    Format a datetime for AEMET API query parameters.
    AEMET expects: 'YYYY-MM-DDTHH:MM:SSUTC'
    """
    utc_dt = dt.astimezone(timezone.utc)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SUTC")


# ── Logging setup ──────────────────────────────────────────────────────────────

def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure root logger for the fetcher scripts.
    Call once from refresh_all.py or CLI entry points.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
