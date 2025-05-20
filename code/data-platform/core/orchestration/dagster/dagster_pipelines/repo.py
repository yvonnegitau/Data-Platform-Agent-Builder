from dagster import FilesystemIOManager, graph, op, repository, schedule, asset
from dagster_docker import docker_executor


@asset
def my_asset():
    return