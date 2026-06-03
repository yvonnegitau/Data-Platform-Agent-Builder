FROM python:3.10-slim

WORKDIR /usr/app

# Install dbt and related packages
RUN pip install --no-cache-dir dbt-postgres==1.5.3 dbt-duckdb==1.5.2

# Install additional utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ssh \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV DBT_PROFILES_DIR=/root/.dbt

# Create directory for profiles
RUN mkdir -p /root/.dbt

# Command to keep container running
CMD ["sleep", "infinity"]