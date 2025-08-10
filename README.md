# Data Platform Agent Builder

A comprehensive data platform for building, orchestrating, and managing data pipelines with intelligent agent-based automation. This system provides a robust foundation for extracting, transforming, and loading data from various sources with built-in monitoring, scheduling, and dependency management.

## 🏗️ Architecture Overview

The Data Platform Agent Builder is built on modern data engineering principles using:

- **Dagster** - Asset-based orchestration and pipeline management
- **DLT (Data Load Tool)** - Flexible data extraction and loading
- **PostgreSQL** - Primary data warehouse
- **Docker** - Containerized deployment
- **Python** - Core development language

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Git

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd Data-Platform-Agent-Builder

# Start the platform
docker compose up --build

# Access the Dagster UI
open http://localhost:3000
```

## 📋 Features

### Core Capabilities

- **Asset-Based Orchestration**: Define data assets with clear dependencies and lineage
- **Intelligent Scheduling**: Configurable cron-based schedules with partition awareness
- **Data Source Integration**: Pre-built connectors for APIs, databases, and files
- **Monitoring & Observability**: Real-time pipeline monitoring and alerting
- **Backfill Support**: Historical data processing with configurable policies
- **I/O Management**: Flexible data passing between pipeline stages

### Example Use Cases

- **Formula 1 Data Pipeline**: Complete F1 season data extraction and processing
- **API Data Integration**: RESTful API data ingestion with rate limiting
- **Reference Data Management**: Static and slowly changing dimension processing
- **Time-Series Analytics**: Partitioned data processing by time windows

## 📂 Project Structure

```
data-platform
│       ├── __init__.py
│       ├── core
│       │   ├── __init__.py
│       │   ├── orchestration
│       │   │   ├── __init__.py
│       │   │   └── dagster
│       │   │       ├── Dockerfile
│       │   │       ├── __init__.py
│       │   │       ├── dagster_pipelines
│       │   │       │   ├── .dlt
│       │   │       │   │   ├── config.toml
│       │   │       │   │   └── secrets.toml
│       │   │       │   ├── __init__.py
│       │   │       │   ├── dlt_ingestion
│       │   │       │   │   ├── __init__.py
│       │   │       │   │   ├── assets
│       │   │       │   │   │   ├── __init__.py
│       │   │       │   │   │   └── f1_assets.py
│       │   │       │   │   ├── pipelines
│       │   │       │   │   │   ├── __init__.py
│       │   │       │   │   │   └── f1_pipelines.py
│       │   │       │   │   └── sources
│       │   │       │   │       ├── __init__.py
│       │   │       │   │       └── f1_source.py
│       │   │       │   └── repo.py
│       │   │       └── requirements.txt
```

## 🛠️ Development
Adding New Data Sources:
1. Create a source definition in `sources/`
2. Define corresponding assets in `assets/`
3. Configure partitioning and schedules
4. Add to job definitions in `repo.py`

## 📈 Monitoring & Operations

Dagster UI Features
* Asset Lineage: Visual representation of data dependencies
* Run History: Detailed execution logs and metrics
* Partition Status: Track completion across time partitions
* Schedule Management: Enable/disable automated runs