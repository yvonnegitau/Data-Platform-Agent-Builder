from datetime import datetime
import json
import logging
import time
from typing import Any, Dict, List, Optional
import logging
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log,
)

logger = logging.getLogger(__name__)


class ErgastClient:
    """
    A client for the Ergast API.
    """

    def __init__(
        self,
        base_url: str = "https://api.jolpi.ca/ergast/f1",
        request_per_second: float = 4.0,
    ):
        self.base_url = base_url
        self.session = requests.Session()
        self.requests_per_second = request_per_second
        self.min_interval = 1.0 / request_per_second
        self.last_request_time = 0

        self.cache = {}

    def _wait_for_rate_limit(self):
        current_time = datetime.now().timestamp()
        elapsed_time = current_time - self.last_request_time

        if elapsed_time < self.min_interval:
            sleep_time = self.min_interval - elapsed_time
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)

    def _get_cache_key(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate a cache key based on the endpoint and parameters.
        """
        if params:
            param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
            return f"{endpoint}?{param_str}"
        return endpoint

    @retry(
        retry=retry_if_exception_type(
            (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.HTTPError,
            )
        ),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.INFO),
        after=after_log(logger, logging.INFO),
        reraise=True,
    )
    def make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Make a GET request to the Ergast API.
        """
        cache_key = self._get_cache_key(endpoint, params)
        if use_cache and cache_key in self.cache:
            logger.debug(f"Cache hit for {cache_key}")
            return self.cache[cache_key]

        url = f"{self.base_url}/{endpoint}.json"

        if params is None:
            params = {}

        self._wait_for_rate_limit()

        try:
            logger.info(f"Making request to {url} with params {params}")
            response = self.session.get(url, params=params)

            if response.status_code == 429:
                # Fix: Handle missing Retry-After header
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        retry_after = int(retry_after)
                        logger.warning(
                            f"Rate limit exceeded. Retrying after {retry_after} seconds."
                        )
                        time.sleep(retry_after)
                    except ValueError:
                        # Handle non-integer Retry-After values
                        default_wait = 5
                        logger.warning(
                            f"Invalid Retry-After header: {retry_after}. Using default wait: {default_wait} seconds."
                        )
                        time.sleep(default_wait)
                else:
                    # Default wait time if no Retry-After header
                    default_wait = 5
                    logger.warning(
                        f"Rate limit exceeded. No Retry-After header found, waiting {default_wait} seconds."
                    )
                    time.sleep(default_wait)
                raise requests.exceptions.HTTPError("Rate limit exceeded, retrying...")

            response.raise_for_status()
            data = response.json()

            if use_cache:
                logger.debug(f"Caching response for {cache_key}")
                self.cache[cache_key] = data

            # Update last request time for rate limiting
            self.last_request_time = datetime.now().timestamp()

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON response: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_seasons(self):
        """Get all F1 seasons with pagination handling."""
        all_seasons = []
        page = 1
        limit = 100  # Maximum allowed by Ergast API

        while True:
            endpoint = "seasons"
            params = {"limit": limit, "offset": (page - 1) * limit}
            logger.info(f"Fetching seasons page {page}, offset: {(page - 1) * limit}")

            response = self.make_request(endpoint, params)

            if not response or "MRData" not in response:
                logger.error(f"Invalid response from API: {response}")
                break

            season_data = (
                response["MRData"]["SeasonTable"]["Seasons"]
                if "SeasonTable" in response["MRData"]
                else []
            )
            logger.info(f"Got {len(season_data)} seasons in page {page}")

            if not season_data:
                break

            all_seasons.extend(season_data)

            # Check if we've reached the end of data
            total = int(response["MRData"]["total"])
            fetched = min(int(response["MRData"]["limit"]), total) * page

            logger.info(f"Progress: fetched {len(all_seasons)}/{total} seasons")

            if fetched >= total:
                logger.info(f"✅ All {total} seasons fetched successfully")
                break

            page += 1

        logger.info(f"Total seasons fetched: {len(all_seasons)}")
        return all_seasons

    @retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_circuits(self) -> List[Dict[str, Any]]:
        """
        Get all circuits.
        """
        all_circuits = []
        page = 1
        limit = 100

        while True:
            endpoint = "circuits"
            params = {"limit": limit, "offset": (page - 1) * limit}
            logger.info(f"Fetching circuits page {page}, offset: {(page - 1) * limit}")
            response = self.make_request(endpoint, params)
            if not response or "MRData" not in response:
                logger.error(f"Invalid response from API: {response}")
                break
            circuit_data = (
                response["MRData"]["CircuitTable"]["Circuits"]
                if "CircuitTable" in response["MRData"]
                else []
            )
            logger.info(f"Got {len(circuit_data)} circuits in page {page}")
            if not circuit_data:
                break
            all_circuits.extend(circuit_data)
            total = int(response["MRData"]["total"])
            fetched = min(int(response["MRData"]["limit"]), total) * page
            logger.info(f"Progress: fetched {len(all_circuits)}/{total} circuits")
            if fetched >= total:
                logger.info(f"✅ All {total} circuits fetched successfully")
                break
            page += 1
        logger.info(f"Total circuits fetched: {len(all_circuits)}")
        return all_circuits

    @retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_races_incremental(self, simulation_date: datetime) -> List[Dict[str, Any]]:

        try:
            current_year = simulation_date.year
            current_round = simulation_date.month

            data = self.make_request(f"{current_year}/races")
            all_races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])

            # TODO Uncomment after backfill
            # filtered_races = []

            # for race in all_races:
            #     race_date = datetime.strptime(f"{race['date']}", "%Y-%m-%d")

            #     if race_date.month <= current_round:
            #         race["data_extracted_at"] = datetime.now().isoformat()
            #         filtered_races.append(race)

            return all_races
        except Exception as e:
            logger.error(f"Error in get_races_incremental: {e}")
            return []

    @retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_results_incremental(
        self, simulation_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get all race results for a specific year with pagination.
        """
        current_year = simulation_date.year
        all_results = []
        page = 1
        limit = 100  # Maximum allowed by Ergast API

        logger.info(f"Fetching results for {current_year} with pagination")

        while True:
            # Use pagination params
            params = {"limit": limit, "offset": (page - 1) * limit}
            logger.info(f"Fetching results page {page}, offset: {(page - 1) * limit}")

            data = self.make_request(f"{current_year}/results", params)
            race_results = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])

            if not race_results:
                logger.info(f"No more race results found on page {page}")
                break

            results_in_page = 0

            # Process each race's results
            for race_data in race_results:
                # Check if the Results key exists
                if "Results" in race_data:
                    race_results_list = race_data["Results"]

                    # Process each individual result from this race
                    for result in race_results_list:
                        # Add race metadata to each result
                        result["season"] = race_data["season"]
                        result["round"] = race_data["round"]
                        result["raceId"] = race_data.get("raceId")
                        result["raceName"] = race_data.get("raceName")
                        result["raceDate"] = race_data.get("date")

                        # Add extraction timestamp
                        result["data_extracted_at"] = datetime.now().isoformat()

                        all_results.append(result)
                        results_in_page += 1

            logger.info(f"Extracted {results_in_page} results from page {page}")

            # Check if we've reached the end of data
            total = int(data["MRData"]["total"])
            fetched = min(int(data["MRData"]["limit"]), total) * page

            logger.info(f"Progress: fetched {len(all_results)}/{total} results")

            if fetched >= total:
                logger.info(f"✅ All {total} results fetched successfully")
                break

            page += 1

        logger.info(
            f"Total race results fetched for {current_year}: {len(all_results)}"
        )
        return all_results

    @retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_drivers_incremental(
        self, simulation_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get all drivers.
        """
        current_year = simulation_date.year
        data = self.make_request(f"{current_year}/drivers")
        all_drivers = data.get("MRData", {}).get("DriverTable", {}).get("Drivers", [])

        for driver in all_drivers:
            driver["data_extracted_at"] = datetime.now().isoformat()
            driver["year"] = current_year
            # Add any additional fields or transformations needed here

        return all_drivers

    @retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_constructors_incremental(
        self, simulation_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get all constructors.
        """
        current_year = simulation_date.year
        data = self.make_request(f"{current_year}/constructors")
        all_constructors = (
            data.get("MRData", {}).get("ConstructorTable", {}).get("Constructors", [])
        )

        for constructor in all_constructors:
            constructor["data_extracted_at"] = datetime.now().isoformat()
            constructor["year"] = current_year
            # Add any additional fields or transformations needed here

        return all_constructors

    @retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_driver_standings_incremental(
        self, simulation_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get driver standings incrementally based on simulation date.
        """
        try:
            current_year = simulation_date.year

            # Get driver standings for the latest round
            data = self.make_request(f"{current_year}/driverStandings")
            standings_lists = (
                data.get("MRData", {})
                .get("StandingsTable", {})
                .get("StandingsLists", [])
            )

            all_driver_standings = []

            for standing_list in standings_lists:
                if "DriverStandings" in standing_list:
                    for driver in standing_list["DriverStandings"]:
                        driver["data_extracted_at"] = datetime.now().isoformat()

                        driver["round"] = standing_list["round"]
                        driver["season"] = standing_list[
                            "season"
                        ]  # Add season for consistency
                        all_driver_standings.append(driver)

            logger.info(f"Retrieved {len(all_driver_standings)} driver standings")
            return all_driver_standings

        except Exception as e:
            logger.error(f"Error in get_driver_standings_incremental: {e}")
            return []

    @retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_constructor_standings_incremental(
        self, simulation_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get constructor standings incrementally based on simulation date.
        """
        try:
            current_year = simulation_date.year

            # Get constructor standings for the latest round
            data = self.make_request(f"{current_year}/constructorStandings")
            standings_lists = (
                data.get("MRData", {})
                .get("StandingsTable", {})
                .get("StandingsLists", [])
            )

            all_constructor_standings = []

            for standing_list in standings_lists:
                if "ConstructorStandings" in standing_list:
                    for constructor in standing_list["ConstructorStandings"]:
                        constructor["data_extracted_at"] = datetime.now().isoformat()
                        constructor["round"] = standing_list["round"]
                        constructor["season"] = standing_list["season"]
                        all_constructor_standings.append(constructor)

            logger.info(
                f"Retrieved {len(all_constructor_standings)} constructor standings"
            )
            return all_constructor_standings

        except Exception as e:
            logger.error(f"Error in get_constructor_standings_incremental: {e}")
            return []

    def clear_cache(self):
        """
        Clear the cache.
        """
        self.cache.clear()
        logger.info("Cache cleared.")

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        """
        return {
            "cache_size": len(self.cache),
            "cache_keys": list(self.cache.keys()),
        }

    def get_simulated_month_data(self, simulation_date: datetime) -> Dict[str, Any]:
        """
        Get all data for a simulated month.
        """

        return {
            "simulation_date": simulation_date.strftime("%Y-%m-%d"),
            "races": self.get_races_incremental(simulation_date),
            "results": self.get_results_incremental(simulation_date),
            "drivers": self.get_drivers_incremental(simulation_date),
            "constructors": self.get_constructors_incremental(simulation_date),
            "driver_standings": self.get_driver_standings_incremental(simulation_date),
            "constructor_standings": self.get_constructor_standings_incremental(
                simulation_date
            ),
        }
