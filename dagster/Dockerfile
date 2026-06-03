FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY dagster/requirements.txt /opt/dagster/app/requirements.txt
RUN pip install --upgrade pip && pip install -r /opt/dagster/app/requirements.txt

RUN mkdir -p /data/medallion && \
    chmod 755 /data/medallion && \
    mkdir -p /opt/dbt/app/target && \
    mkdir -p /opt/dagster/app/storage && \
    mkdir -p /opt/dagster/app/logs && \
    mkdir -p /tmp/build-logs

COPY dagster/pipelines/ /opt/dagster/app/
COPY dbt/ /opt/dbt/app/


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
RUN dbt compile --profiles-dir /opt/dbt/app --profile data_platform_f1 2>&1 | tee /tmp/build-logs/dbt-compile.log \
    || (cat /tmp/build-logs/dbt-compile.log && echo "dbt compile failed — will retry at runtime")

WORKDIR /opt/dagster/app

EXPOSE 4000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "repo.py"]