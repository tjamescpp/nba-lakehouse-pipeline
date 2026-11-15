import random
import time
from datetime import date

import requests

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"

# Transient failures worth retrying. Other errors (e.g. 404) won't fix themselves.
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def _get_with_retries(url: str, params: dict, max_retries: int = 5,
                      base_delay: float = 2.0, timeout: int = 30) -> str:
    """GET a URL and return the raw response body, retrying transient failures."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as exc:
            error = repr(exc)
        else:
            if resp.status_code == 200:
                return resp.text
            error = f"HTTP {resp.status_code}"
            if resp.status_code not in RETRYABLE_STATUS:
                raise RuntimeError(f"{url} failed with non-retryable {error}")

        if attempt == max_retries:
            raise RuntimeError(f"{url} failed after {max_retries} attempts: {error}")

        # exponential backoff + jitter: ~2s, 4s, 8s, 16s
        delay = base_delay * 2 ** (attempt - 1) + random.uniform(0, 1)
        print(f"attempt {attempt} failed ({error}); retrying in {delay:.1f}s")
        time.sleep(delay)


def fetch_scoreboard_json(game_date: date) -> str:
    """Raw scoreboard JSON for one date: the list of games played that day."""
    return _get_with_retries(SCOREBOARD_URL, {"dates": game_date.strftime("%Y%m%d")})


def fetch_game_summary_json(game_id: str) -> str:
    """Raw summary JSON for one game: includes the player box score."""
    return _get_with_retries(SUMMARY_URL, {"event": game_id})