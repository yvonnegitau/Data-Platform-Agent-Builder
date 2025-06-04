FROM python:3.10-slim

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagster-webserver and dagster-daemon, and to load the DagsterInstance

# Install dependencies
COPY code/data-platform/core/orchestration/dagster/requirements.txt /opt/dagster/app/requirements.txt
RUN pip install -r /opt/dagster/app/requirements.txt

# Create the medallion directory structure
RUN mkdir -p /data/medallion
RUN chmod 755 /data/medallion

WORKDIR /opt/dagster/app



COPY code/data-platform/core/orchestration/dagster/dagster_pipelines/ /opt/dagster/app


# Run dagster gRPC server on port 4000

EXPOSE 4000


# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "repo.py" ]