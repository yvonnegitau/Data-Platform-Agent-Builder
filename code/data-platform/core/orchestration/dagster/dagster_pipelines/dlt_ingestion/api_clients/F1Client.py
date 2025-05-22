from datetime import datetime
import json
from typing import Any, Dict, List, Optional
from venv import logger
import requests


class ErgastClient:
    """
    A client for the Ergast API.
    """

    def __init__(self, base_url: str = "https://api.jolpi.ca/ergast/f1"):
        self.base_url = base_url
        self.session = requests.Session()

    def make_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make a GET request to the Ergast API.
        """
        url = f"{self.base_url}/{endpoint}.json"

        if params is None:
            params = {}
        if "limit" not in params:
            params["limit"] = 100

        try:

            logger.info(f"Making request to {url} with params {params}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
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

    def paginated_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Make a paginated request to the Ergast API.
        """
        if params is None:
            params = {}

        limit = params.get("limit", 100)
        offset = 0
        all_data = []
        total = None

        while total is None or offset < total:
            request_params = {**params, "offset": offset, "limit": limit}
            data = self.make_request(endpoint, request_params)

            mr_data = data.get("MRData", {})

            if total is None:
                total = int(mr_data.get("total", "0"))

            table_key = next(
                (key for key in mr_data.keys() if key.endswith("Table")), None
            )

            if table_key:
                items_key = next(
                    (
                        key
                        for key in mr_data.get(table_key, {}).keys()
                        if key not in ["season", "round"]
                    ),
                    None,
                )
                if items_key:
                    items = mr_data[table_key].get(items_key, [])
                    all_data.extend(items)

                    if len(items) < limit:
                        break

            offset += limit

        return all_data

    def get_seasons(self) -> List[Dict[str, Any]]:
        """
        Get all seasons.
        """
        data = self.make_request("seasons")
        return data.get("MRData", {}).get("SeasonTable", {}).get("Seasons", [])

    def get_circuits(self) -> List[Dict[str, Any]]:
        """
        Get all circuits.
        """
        data = self.make_request("circuits")
        return data.get("MRData", {}).get("CircuitTable", {}).get("Circuits", [])

    def get_races_incremental(self, simulation_date: datetime) -> List[Dict[str, Any]]:

        current_year = simulation_date.year
        current_round = simulation_date.month

        data = self.make_request(f"{current_year}/races/")
        all_races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])

        filtered_races = []

        for race in all_races:
            race_date = datetime.strptime(f"{race['date']}", "%Y-%m-%d")

            if race_date.month <= current_round:
                filtered_races.append(race)

        return filtered_races

    def get_results_incremental(
        self, simulation_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get all results.
        """

        races = self.get_races_incremental(simulation_date)
        all_results = []

        for races in races:
            season = races["season"]
            round_num = races["round"]

            data = self.make_request(f"{season}/{round_num}/results")
            race_results = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])

            for race_data in race_results:
                if "Results" in race_data:
                    for result in race_data["Results"]:
                        result["season"] = season
                        result["round"] = round_num
                        result["raceId"] = race_data.get("raceId")
                        result["raceName"] = race_data.get("raceName")
                        result["raceDate"] = race_data.get("date")
                        all_results.append(result)

        return all_results

    def get_drivers_incremental(
        self, simulation_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get all drivers.
        """
        current_year = simulation_date.year
        data = self.make_request(f"{current_year}/drivers")
        all_drivers = data.get("MRData", {}).get("DriverTable", {}).get("Drivers", [])

        return all_drivers

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

        return all_constructors

    def get_standings_incremental(
        self, simulation_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get all standings.
        """
        current_year = simulation_date.year

        races = self.get_races_incremental(simulation_date)

        if not races:
            return {"driverStandings": [], "constructorStandings": []}

        latest_round = max(int(race["round"]) for race in races)

        driver_data = self.make_request(
            f"{current_year}/{latest_round}/driverStandings"
        )
        constructor_data = self.make_request(
            f"{current_year}/{latest_round}/constructorStandings"
        )
        driver_standings = (
            driver_data.get("MRData", {})
            .get("StandingsTable", {})
            .get("StandingsLists", [])
        )
        constructor_standings = (
            constructor_data.get("MRData", {})
            .get("StandingsTable", {})
            .get("StandingsLists", [])
        )

        return {
            "driverStandings": driver_standings,
            "constructorStandings": constructor_standings,
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
            "standings": self.get_standings_incremental(simulation_date),
        }
