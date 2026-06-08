from datetime import datetime
import time
import threading
import logging
from typing import Any, Dict, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import dlt
import dagster
import requests

from dagster_dlt import DagsterDltResource
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.sources.helpers.rest_client import RESTClient
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

_MAX_WORKERS = 5         # parallel round fetches
_REQUESTS_PER_SEC = 3    # global cap across all threads


class _RateLimiter:
    """Token bucket: allows up to `rate` requests per second across all threads."""
    def __init__(self, rate: float):
        self._rate = rate
        self._lock = threading.Lock()
        self._last = time.monotonic()

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            gap = 1.0 / self._rate
            wait = self._last + gap - now
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()


_rate_limiter = _RateLimiter(_REQUESTS_PER_SEC)


def _make_client(base_url: str, data_selector: str) -> RESTClient:
    return RESTClient(
        base_url=base_url,
        paginator=OffsetPaginator(limit=100, offset=0, total_path="MRData.total"),
        data_selector=data_selector,
    )


@retry(
    retry=retry_if_exception_type(requests.HTTPError),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _fetch_pages(client: RESTClient, endpoint: str) -> List[Any]:
    pages = []
    for page in client.paginate(endpoint):
        pages.append(page)
    return pages


def _fetch_round_laps(base_url: str, year: int, round_num: int) -> List[Dict]:
    client = _make_client(base_url, "MRData.RaceTable.Races")
    rows = []
    try:
        for page in _fetch_pages(client, f"{year}/{round_num}/laps.json"):
            if page and "Laps" in page[0]:
                for lap in page[0]["Laps"]:
                    lap["season"] = page[0]["season"]
                    lap["round"] = page[0]["round"]
                    lap["date_extracted_at"] = datetime.now().isoformat()
                    rows.append(lap)
    except Exception as e:
        logger.warning(f"Failed to fetch laps for {year} round {round_num}: {e}")
    return rows


@dlt.source(name="f1_source")
def f1_api_source(
    base_url: str = "https://api.jolpi.ca/ergast/f1",
    years: Optional[Union[List[int], range]] = None,
    rounds: Optional[List[int]] = None,
) -> List[DagsterDltResource]:
    if years is None:
        years = [datetime.now().year]

    @dlt.resource(name="seasons", primary_key="season", write_disposition="merge")
    def seasons():
        log = dagster.get_dagster_logger()
        client = _make_client(base_url, "MRData.SeasonTable.Seasons")
        for page in _fetch_pages(client, "/seasons.json"):
            log.info(f"Extracted {len(page)} seasons")
            for row in page:
                row["date_extracted_at"] = datetime.now().isoformat()
                yield row

    @dlt.resource(name="circuits", primary_key="circuitId", write_disposition="merge")
    def circuits():
        log = dagster.get_dagster_logger()
        client = _make_client(base_url, "MRData.CircuitTable.Circuits")
        for page in _fetch_pages(client, "/circuits.json"):
            log.info(f"Extracted {len(page)} circuits")
            for row in page:
                row["date_extracted_at"] = datetime.now().isoformat()
                yield row

    @dlt.resource(name="status", primary_key="statusId", write_disposition="merge")
    def status():
        log = dagster.get_dagster_logger()
        client = _make_client(base_url, "MRData.StatusTable.Status")
        for page in _fetch_pages(client, "/status.json"):
            log.info(f"Extracted {len(page)} status entries")
            for row in page:
                row["date_extracted_at"] = datetime.now().isoformat()
                yield row

    @dlt.resource(
        name="drivers", primary_key=["driverId", "year"], write_disposition="merge"
    )
    def drivers(year: Optional[int] = None):
        log = dagster.get_dagster_logger()
        years_to_process = [year] if year is not None else years
        for yr in years_to_process:
            log.info(f"Processing drivers for {yr}")
            client = _make_client(base_url, "MRData.DriverTable.Drivers")
            for page in _fetch_pages(client, f"{yr}/drivers.json"):
                for row in page:
                    row["year"] = yr
                    row["date_extracted_at"] = datetime.now().isoformat()
                    yield row
            _rate_limiter.acquire()

    @dlt.resource(
        name="constructors",
        primary_key=["constructorId", "year"],
        write_disposition="merge",
    )
    def constructors(year: Optional[int] = None):
        log = dagster.get_dagster_logger()
        years_to_process = [year] if year is not None else years
        for yr in years_to_process:
            log.info(f"Processing constructors for {yr}")
            client = _make_client(base_url, "MRData.ConstructorTable.Constructors")
            for page in _fetch_pages(client, f"{yr}/constructors.json"):
                for row in page:
                    row["year"] = yr
                    row["date_extracted_at"] = datetime.now().isoformat()
                    yield row
            _rate_limiter.acquire()

    @dlt.resource(
        name="races", primary_key=["season", "round"], write_disposition="merge"
    )
    def races(year: Optional[int] = None):
        log = dagster.get_dagster_logger()
        years_to_process = [year] if year is not None else years
        for yr in years_to_process:
            log.info(f"Processing races for {yr}")
            client = _make_client(base_url, "MRData.RaceTable.Races")
            for page in _fetch_pages(client, f"{yr}/races.json"):
                for row in page:
                    row["date_extracted_at"] = datetime.now().isoformat()
                    yield row
            _rate_limiter.acquire()

    @dlt.resource(
        name="results",
        primary_key=["season", "round", "number", "constructor__constructor_id"],
        write_disposition="merge",
    )
    def results(year: Optional[int] = None):
        log = dagster.get_dagster_logger()
        years_to_process = [year] if year is not None else years
        for yr in years_to_process:
            log.info(f"Processing results for {yr}")
            client = _make_client(base_url, "MRData.RaceTable.Races")
            for page in _fetch_pages(client, f"{yr}/results"):
                for race in page:
                    if not race or "Results" not in race:
                        continue
                    for row in race["Results"]:
                        row["season"] = race["season"]
                        row["round"] = race["round"]
                        row["date_extracted_at"] = datetime.now().isoformat()
                        yield row
                _rate_limiter.acquire()

    @dlt.resource(
        name="driver_standings",
        primary_key=["season", "round", "driver__driver_id"],
        write_disposition="merge",
    )
    def driver_standings(year: Optional[int] = None):
        log = dagster.get_dagster_logger()
        years_to_process = [year] if year is not None else years
        for yr in years_to_process:
            log.info(f"Processing driver standings for {yr}")
            client = _make_client(base_url, "MRData.StandingsTable.StandingsLists")
            seen: set = set()
            for page in _fetch_pages(client, f"{yr}/driverStandings"):
                for standings_list in page:
                    if not standings_list or "DriverStandings" not in standings_list:
                        continue
                    season = standings_list["season"]
                    round_ = standings_list["round"]
                    for row in standings_list["DriverStandings"]:
                        key = (season, round_, row.get("Driver", {}).get("driverId", ""))
                        if key in seen:
                            continue
                        seen.add(key)
                        row["season"] = season
                        row["round"] = round_
                        row["date_extracted_at"] = datetime.now().isoformat()
                        yield row
            _rate_limiter.acquire()

    @dlt.resource(
        name="constructor_standings",
        primary_key=["season", "round", "constructor__constructor_id"],
        write_disposition="merge",
    )
    def constructor_standings(year: Optional[int] = None):
        log = dagster.get_dagster_logger()
        years_to_process = [year] if year is not None else years
        for yr in years_to_process:
            log.info(f"Processing constructor standings for {yr}")
            client = _make_client(base_url, "MRData.StandingsTable.StandingsLists")
            seen: set = set()
            for page in _fetch_pages(client, f"{yr}/constructorStandings"):
                for standings_list in page:
                    if not standings_list or "ConstructorStandings" not in standings_list:
                        continue
                    season = standings_list["season"]
                    round_ = standings_list["round"]
                    for row in standings_list["ConstructorStandings"]:
                        key = (season, round_, row.get("Constructor", {}).get("constructorId", ""))
                        if key in seen:
                            continue
                        seen.add(key)
                        row["season"] = season
                        row["round"] = round_
                        row["date_extracted_at"] = datetime.now().isoformat()
                        yield row
            _rate_limiter.acquire()

    @dlt.resource(
        name="laps",
        primary_key=["season", "round", "number"],
        write_disposition="merge",
    )
    def laps(year: Optional[int] = None, rounds_override: Optional[List[int]] = None):
        log = dagster.get_dagster_logger()
        years_to_process = [year] if year is not None else years
        rounds_to_use = rounds_override if rounds_override is not None else (rounds or [])

        for yr in years_to_process:
            if not rounds_to_use:
                log.warning(f"No rounds specified for laps in {yr}, skipping")
                continue

            log.info(f"Fetching laps for {yr}, {len(rounds_to_use)} rounds in parallel")
            with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
                futures = {
                    pool.submit(_fetch_round_laps, base_url, yr, r): r
                    for r in rounds_to_use
                }
                for future in as_completed(futures):
                    round_num = futures[future]
                    try:
                        for row in future.result():
                            yield row
                        log.info(f"Completed laps for {yr} round {round_num}")
                    except Exception as e:
                        log.error(f"Error fetching laps for {yr} round {round_num}: {e}")

    @dlt.resource(
        name="qualifying",
        primary_key=["season", "round", "number"],
        write_disposition="merge",
    )
    def qualifying(year: Optional[int] = None):
        log = dagster.get_dagster_logger()
        years_to_process = [year] if year is not None else years
        for yr in years_to_process:
            log.info(f"Processing qualifying for {yr}")
            client = _make_client(base_url, "MRData.RaceTable.Races")
            for page in _fetch_pages(client, f"{yr}/qualifying"):
                for race in page:
                    if not race or "QualifyingResults" not in race:
                        continue
                    for row in race["QualifyingResults"]:
                        row["season"] = race["season"]
                        row["round"] = race["round"]
                        row["date_extracted_at"] = datetime.now().isoformat()
                        yield row
            _rate_limiter.acquire()

    return (
        seasons,
        circuits,
        drivers,
        constructors,
        races,
        results,
        driver_standings,
        constructor_standings,
        laps,
        qualifying,
        status,
    )
