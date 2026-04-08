"""
Retry decorator for HTTP requests with exponential backoff.
"""
import time
import functools
import logging

import requests

logger = logging.getLogger(__name__)


def retry_request(max_attempts=3, backoff_base=1.0,
                  status_forcelist=(429, 500, 502, 503, 504)):
    """
    Decorator for functions that make HTTP requests.

    On requests.HTTPError with status in status_forcelist: exponential backoff + retry.
    On 429 (rate limit): sleep 60s before retry.
    Raises RuntimeError after max_attempts exhausted.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except requests.HTTPError as e:
                    last_exc = e
                    status = e.response.status_code if e.response is not None else 0
                    if status == 429:
                        logger.warning(
                            "%s: rate limited (429), sleeping 60s (attempt %d/%d)",
                            fn.__name__, attempt + 1, max_attempts
                        )
                        time.sleep(60)
                    elif status in status_forcelist:
                        sleep_time = backoff_base * (2 ** attempt)
                        logger.warning(
                            "%s: HTTP %d, retrying in %.1fs (attempt %d/%d)",
                            fn.__name__, status, sleep_time, attempt + 1, max_attempts
                        )
                        time.sleep(sleep_time)
                    else:
                        raise
                except (requests.ConnectionError, requests.Timeout) as e:
                    last_exc = e
                    sleep_time = backoff_base * (2 ** attempt)
                    logger.warning(
                        "%s: connection error, retrying in %.1fs (attempt %d/%d): %s",
                        fn.__name__, sleep_time, attempt + 1, max_attempts, e
                    )
                    time.sleep(sleep_time)
            raise RuntimeError(
                f"{fn.__name__} failed after {max_attempts} attempts"
            ) from last_exc
        return wrapper
    return decorator
