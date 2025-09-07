FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /opt/dagster/app

# Copy and install Python dependencies
COPY code/data-platform/core/orchestration/dagster/requirements.txt /opt/dagster/app/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Create directory structure with proper ownership
RUN mkdir -p /opt/dbt/app/target && \
    mkdir -p /opt/dagster/app/storage && \
    mkdir -p /opt/dagster/app/logs && \
    mkdir -p /tmp/build-logs 
# Copy application code and set ownership
COPY code/data-platform/core/orchestration/dagster/dagster_pipelines/ /opt/dagster/app/
COPY code/data-platform/core/processing/dbt/data_platform_f1/ /opt/dbt/app/


# Set environment variables
ENV DBT_PROJECT_DIR=/opt/dbt/app
ENV DBT_PROFILES_DIR=/opt/dbt/app
ENV DAGSTER_HOME=/opt/dagster/app

# Build args and env vars for Postgres
ARG DAGSTER_POSTGRES_HOST
ARG DAGSTER_POSTGRES_PORT  
ARG DAGSTER_POSTGRES_DB
ARG DAGSTER_POSTGRES_USER
ARG DAGSTER_POSTGRES_PASSWORD

ENV DAGSTER_POSTGRES_HOST=${DAGSTER_POSTGRES_HOST}
ENV DAGSTER_POSTGRES_PORT=${DAGSTER_POSTGRES_PORT}
ENV DAGSTER_POSTGRES_DB=${DAGSTER_POSTGRES_DB}
ENV DAGSTER_POSTGRES_USER=${DAGSTER_POSTGRES_USER}
ENV DAGSTER_POSTGRES_PASSWORD=${DAGSTER_POSTGRES_PASSWORD}

# Prepare dbt project
WORKDIR /opt/dbt/app
RUN dbt deps --profiles-dir /opt/dbt/app || echo "No packages to install"
RUN dbt parse --profiles-dir /opt/dbt/app

RUN echo "=== DBT COMPILE START ===" && \
    dbt compile --profiles-dir /opt/dbt/app --profile data_platform_f1 2>&1 | tee /tmp/build-logs/dbt-compile.log && \
    echo "=== DBT COMPILE COMPLETE ===" && \
    echo "=== DBT COMPILE LOG ===" && \
    cat /tmp/build-logs/dbt-compile.log && \
    echo "=== END DBT COMPILE LOG ===" || \
    (echo "=== DBT COMPILE FAILED ===" && \
     echo "=== ERROR LOG ===" && \
     cat /tmp/build-logs/dbt-compile.log && \
     echo "=== END ERROR LOG ===" && \
     echo "Will retry at runtime")

WORKDIR /opt/dagster/app

EXPOSE 4000



CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "repo.py" ]