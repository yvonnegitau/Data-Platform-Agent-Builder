FROM python:3.10-slim


WORKDIR /opt/dagster/app

# Install dependencies
COPY infrastructure/dagster.requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY infrastructure/dagster.yaml .
COPY infrastructure/workspace.yaml .


# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/app
ENV PYTHONPATH=/opt/dagster/app/:${PYTHONPATH}