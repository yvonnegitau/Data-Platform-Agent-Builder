from datetime import datetime
import time
import logging
from typing import Any, Dict, List, Optional, Union
import dlt
import dagster


from dagster_dlt import DagsterDltResource

from requests.models import Response
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.sources.helpers.rest_client import RESTClient

logger = logging.getLogger(__name__)


@dlt.source(name="f1_source")
def f1_api_source(
    base_url: str = "https://api.jolpi.ca/ergast/f1",
    years: Optional[Union[List[int], range]] = None,
    rounds: Optional[int] = None,
) -> List[DagsterDltResource]:
    """
    Source for the F1 API.
    """
    if years is None:
        years = [datetime.now().year]

    @dlt.resource(
        name="seasons",
        primary_key="season",
        write_disposition="merge",
    )
    def seasons():
        logger = dagster.get_dagster_logger()
        logger.info("Processing F1 seasons")

        # Create a REST client with the correct paginator
        client = RESTClient(
            base_url=base_url,
            paginator=OffsetPaginator(
                limit=100,
                offset=0,
                total_path="MRData.total",
            ),
            data_selector="MRData.SeasonTable.Seasons",
        )

        # Make the request with pagination
        for page in client.paginate(
            "/seasons.json",
        ):
            logger.info(f"Extracted {len(page)} seasons")
            for season in page:

                yield season

    @dlt.resource(
        name="circuits",
        primary_key="circuitId",
        write_disposition="merge",
    )
    def circuits():
        """
        Resource for F1 circuits.
        """
        logger = dagster.get_dagster_logger()
        logger.info("Processing F1 circuits")

        # Create a REST client with the correct paginator
        client = RESTClient(
            base_url=base_url,
            paginator=OffsetPaginator(
                limit=100,
                offset=0,
                total_path="MRData.total",
            ),
            data_selector="MRData.CircuitTable.Circuits",
        )

        # Make the request with pagination
        for page in client.paginate(
            "/circuits.json",
        ):
            logger.info(f"Extracted {len(page)} circuits")

            for circuit in page:
                yield circuit

    @dlt.resource(
        name="drivers",
        primary_key=["driverId", "year"],
        write_disposition="merge",
    )
    def drivers(year: Optional[int] = None):
        """
        Resource for F1 drivers.
        """
        logger = dagster.get_dagster_logger()
        years_to_process = [year] if year is not None else years

        logger.info(f"Processing F1 drivers for years: {years_to_process}")
        for year in years_to_process:
            logger.info(f"Processing F1 drivers for year: {year}")

            # Create a REST client with the correct paginator
            client = RESTClient(
                base_url=base_url,
                paginator=OffsetPaginator(
                    limit=100,
                    offset=0,
                    total_path="MRData.total",
                ),
                data_selector="MRData.DriverTable.Drivers",
            )

            # Make the request with pagination
            for page in client.paginate(
                f"{year}/drivers.json",
            ):
                logger.info(f"Extracted {len(page)} drivers for year {year}")

                for driver in page:
                    driver["year"] = year
                    driver["data_extracted_at"] = datetime.now().isoformat()
                    yield driver
            time.sleep(30)  # Respect API rate limits

    @dlt.resource(
        name="constructors",
        primary_key=["constructorId", "year"],
        write_disposition="merge",
    )
    def constructors(year: Optional[int] = None):
        """
        Resource for F1 constructors.
        """
        years_to_process = [year] if year is not None else years
        logger = dagster.get_dagster_logger()
        logger.info(f"Processing F1 constructors for years: {years_to_process}")
        for year in years_to_process:
            logger.info(f"Processing F1 constructors for year: {year}")

            # Create a REST client with the correct paginator
            client = RESTClient(
                base_url=base_url,
                paginator=OffsetPaginator(
                    limit=100,
                    offset=0,
                    total_path="MRData.total",
                ),
                data_selector="MRData.ConstructorTable.Constructors",
            )

            # Make the request with pagination
            for page in client.paginate(
                f"{year}/constructors.json",
            ):
                logger.info(f"Extracted {len(page)} constructors for year {year}")

                for constructor in page:
                    constructor["year"] = year
                    constructor["data_extracted_at"] = datetime.now().isoformat()
                    yield constructor
            time.sleep(30)  # Respect API rate limits

    @dlt.resource(
        name="races",
        primary_key=["season", "round"],
        write_disposition="merge",
    )
    def races(year: Optional[int] = None):
        """
        Resource for races.
        """
        years_to_process = [year] if year is not None else years
        logger = dagster.get_dagster_logger()
        logger.info(f"Processing F1 races for years: {years_to_process}")
        for year in years_to_process:
            logger.info(f"Processing races for {year}")
            # Create a REST client with the correct paginator
            client = RESTClient(
                base_url=base_url,
                paginator=OffsetPaginator(
                    limit=100,
                    offset=0,
                    total_path="MRData.total",
                ),
                data_selector="MRData.RaceTable.Races",
            )
            # Make the request with pagination
            for page in client.paginate(
                f"{year}/races.json",
            ):
                logger.info(f"Extracted {len(page)} pages for year {year}")

                for race in page:
                    race["extracted_at"] = datetime.now().isoformat()
                    yield race
            time.sleep(30)

    @dlt.resource(
        name="results",
        primary_key=["season", "round", "number"],
        write_disposition="merge",
    )
    def results(year: Optional[int] = None):
        """Resource for Results"""
        years_to_process = [year] if year is not None else years
        logger = dagster.get_dagster_logger()

        logger.info(f"Processing F1 results for years: {years_to_process}")
        for year in years_to_process:
            logger.info(f"Processing results for {year}")

            # Create a REST client with the correct paginator
            client = RESTClient(
                base_url=base_url,
                paginator=OffsetPaginator(
                    limit=100,
                    offset=0,
                    total_path="MRData.total",
                ),
                data_selector="MRData.RaceTable.Races",
            )
            #        # Make the request with pagination
            for page in client.paginate(
                f"{year}/results",
            ):
                logger.info(f"Extracted {len(page)} results for year {year}")
                if len(page) == 0:
                    logger.warning(f"No Results data found for year {year}")
                    continue
                if "Results" in page[0]:
                    logger.info(
                        f"Processing results for season {page[0]['season']} and round {page[0]['round']}"
                    )

                    for result in page[0]["Results"]:
                        result["season"] = page[0]["season"]
                        result["round"] = page[0]["round"]

                        result["data_extracted_at"] = datetime.now().isoformat()
                        yield result
                time.sleep(30)

    @dlt.resource(
        name="driver_standings",
        primary_key=["season", "round", "driver__driver_id"],
        write_disposition="merge",
    )
    def driver_standings(year: Optional[int] = None):
        """
        Resource for Driver Standings.
        """
        years_to_process = [year] if year is not None else years
        logger = dagster.get_dagster_logger()
        logger.info(f"Processing F1 driver standings for years: {years_to_process}")
        for year in years_to_process:
            logger.info(f"Processing driver standings for {year}")

            # Create a REST client with the correct paginator
            client = RESTClient(
                base_url=base_url,
                paginator=OffsetPaginator(
                    limit=100,
                    offset=0,
                    total_path="MRData.total",
                ),
                data_selector="MRData.StandingsTable.StandingsLists",
            )

            # Make the request with pagination
            for page in client.paginate(
                f"{year}/driverStandings",
            ):
                logger.info(f"Extracted {len(page)} driver standings for year {year}")
                if len(page) == 0:
                    logger.warning(f"No Driver standings data found for year {year}")
                    continue
                if "DriverStandings" in page[0]:
                    for standing in page[0]["DriverStandings"]:
                        standing["season"] = page[0]["season"]
                        standing["round"] = page[0]["round"]
                        standing["data_extracted_at"] = datetime.now().isoformat()
                        yield standing
            time.sleep(30)  # Respect API rate limits

    @dlt.resource(
        name="constructor_standings",
        primary_key=["season", "round", "constructor__constructor_id"],
        write_disposition="merge",
    )
    def constructor_standings(year: Optional[int] = None):
        """
        Resource for Constructor Standings.
        """
        years_to_process = [year] if year is not None else years
        logger = dagster.get_dagster_logger()
        logger.info(
            f"Processing F1 constructor standings for years: {years_to_process}"
        )
        for year in years_to_process:
            logger.info(f"Processing constructor standings for {year}")

            # Create a REST client with the correct paginator
            client = RESTClient(
                base_url=base_url,
                paginator=OffsetPaginator(
                    limit=100,
                    offset=0,
                    total_path="MRData.total",
                ),
                data_selector="MRData.StandingsTable.StandingsLists",
            )

            # Make the request with pagination
            for page in client.paginate(
                f"{year}/constructorStandings",
            ):
                logger.info(
                    f"Extracted {len(page)} constructor standings for year {year}"
                )
                logger.info(f"Constructor standings data: {page}")
                if len(page) == 0:
                    logger.warning(
                        f"No constructor standings data found for year {year}"
                    )
                    continue
                if "ConstructorStandings" in page[0]:
                    for standing in page[0]["ConstructorStandings"]:
                        standing["season"] = page[0]["season"]
                        standing["round"] = page[0]["round"]
                        standing["data_extracted_at"] = datetime.now().isoformat()
                        yield standing
            time.sleep(30)

    @dlt.resource(
        name="laps",
        primary_key=["season", "round", "number"],
        write_disposition="merge",
    )
    def laps(year: Optional[int] = None):
        """
        Resource for Laps.
        """
        years_to_process = [year] if year is not None else years
        logger = dagster.get_dagster_logger()
        logger.info(f"Processing F1 laps for years: {years_to_process}")
        for year in years_to_process:
            logger.info(f"Processing laps for {year}")

            # Create a REST client with the correct paginator
            client = RESTClient(
                base_url=base_url,
                paginator=OffsetPaginator(
                    limit=100,
                    offset=0,
                    total_path="MRData.total",
                ),
                data_selector="MRData.RaceTable.Races",
            )
            for round in rounds:
                # Make the request with pagination
                for page in client.paginate(
                    f"{year}/{round}/laps.json",
                ):
                    logger.info(f"Extracted {len(page)} laps for year {year}")
                    if len(page) == 0:
                        logger.warning(f"No laps data found for year {year}")
                        continue
                    if "Laps" in page[0]:
                        for lap in page[0]["Laps"]:
                            lap["season"] = page[0]["season"]
                            lap["round"] = page[0]["round"]
                            lap["data_extracted_at"] = datetime.now().isoformat()
                            yield lap
                time.sleep(30)  # Respect API rate limits

    @dlt.resource(
        name="qualifying",
        primary_key=["season", "round", "number"],
        write_disposition="merge",
    )
    def qualifying(year: Optional[int] = None):
        """
        Resource for Qualifying.
        """
        years_to_process = [year] if year is not None else years
        logger = dagster.get_dagster_logger()
        logger.info(f"Processing F1 qualifying for years: {years_to_process}")
        for year in years_to_process:
            logger.info(f"Processing qualifying for {year}")

            # Create a REST client with the correct paginator
            client = RESTClient(
                base_url=base_url,
                paginator=OffsetPaginator(
                    limit=100,
                    offset=0,
                    total_path="MRData.total",
                ),
                data_selector="MRData.RaceTable.Races",
            )
            # Make the request with pagination
            for page in client.paginate(
                f"{year}/qualifying",
            ):
                logger.info(f"Extracted {len(page)} qualifying for year {year}")
                if len(page) == 0:
                    logger.warning(f"No qualifying data found for year {year}")
                    continue
                if "QualifyingResults" in page[0]:
                    for qualifying in page[0]["QualifyingResults"]:
                        qualifying["season"] = page[0]["season"]
                        qualifying["round"] = page[0]["round"]
                        qualifying["data_extracted_at"] = datetime.now().isoformat()
                        yield qualifying
            time.sleep(30)  # Respect API rate limits

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
    )
