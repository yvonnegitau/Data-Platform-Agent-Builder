from datetime import datetime
import logging
from typing import Any, Dict, List, Optional
import dlt
from dlt_ingestion.api_clients.F1Client import ErgastClient

logger = logging.getLogger(__name__)


@dlt.source
def f1_bronze_data(simulation_date: Optional[datetime] = None):

    if simulation_date is None:
        simulation_date = datetime.now().year

    client = ErgastClient()

    return [
        dlt.resource(name="races", primary_key=["season", "round"])(
            lambda: client.get_races_incremental(simulation_date)
        ),
        dlt.resource(name="results", primary_key=["season", "round", "driverId"])(
            lambda: client.get_results_incremental(simulation_date)
        ),
        dlt.resource(name="drivers", primary_key="driverId")(
            lambda: client.get_drivers_incremental(simulation_date)
        ),
        dlt.resource(name="constructors", primary_key="constructorId")(
            lambda: client.get_constructors_incremental(simulation_date)
        ),
        dlt.resource(
            name="driver_standings", primary_key=["season", "round", "driverId"]
        )(
            lambda: extract_driver_standings(
                client.get_standings_incremental(simulation_date)
            )
        ),
        dlt.resource(
            name="constructor_standings",
            primary_key=["season", "round", "constructorId"],
        )(
            lambda: extract_constructor_standings(
                client.get_standings_incremental(simulation_date)
            )
        ),
    ]


def extract_driver_standings(standings_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    all_standings = []

    for standings_list in standings_data:

        season = standings_list.get("season")
        round_num = standings_list.get("round")

        for standing in standings_list.get("DriverStandings", []):
            standing["season"] = season
            standing["round"] = round_num

            all_standings.append(standing)

    return all_standings


def extract_constructor_standings(
    standings_data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    all_standings = []

    for standings_list in standings_data:

        season = standings_list.get("season")
        round_num = standings_list.get("round")

        for standing in standings_list.get("ConstructorStandings", []):
            standing["season"] = season
            standing["round"] = round_num

            all_standings.append(standing)

    return all_standings
