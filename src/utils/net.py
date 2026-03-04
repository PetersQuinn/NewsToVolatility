import time
import random
import requests

def get_with_retries(url: str, params: dict, timeout: int = 30, max_tries: int = 6, base_sleep: float = 1.0):
    """
    Gentle network helper:
    - exponential backoff
    - jitter
    - handles 429/5xx by retrying
    """
    for attempt in range(1, max_tries + 1):
        resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code == 200:
            return resp

        if resp.status_code in (429, 500, 502, 503, 504):
            sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            time.sleep(sleep_s)
            continue

        # Non-retryable
        resp.raise_for_status()

    raise RuntimeError(f"Failed after {max_tries} tries: {url}")