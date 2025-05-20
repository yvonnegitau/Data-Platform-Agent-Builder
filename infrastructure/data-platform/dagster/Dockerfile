FROM python:3.10-slim

WORKDIR /opt/dagster/app

# Install dependencies
COPY infrastructure/data-platform/dagster/requirements.txt .
RUN pip install -r requirements.txt

# Copy dagster configuration
COPY infrastructure/data-platform/dagster/dagster.yaml .
COPY infrastructure/data-platform/dagster/workspace.yaml .

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/app
ENV PYTHONPATH=/opt/dagster/app/code:${PYTHONPATH}

# Setup working directory
WORKDIR /opt/dagster/app



