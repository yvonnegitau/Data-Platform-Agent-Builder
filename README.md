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

## 🏢 Project Structure

```
Data-Platform-Agent-Builder/
├── code/
│   └── data-platform/
│       └── core/
│           └── orchestration/
│               └── dagster/
│                   └── dagster_pipelines/
│                       ├── dlt_ingestion/         # DLT-based data ingestion
│                       │   ├── assets/            # Dagster assets
│                       │   ├── sources/           # Data source definitions
│                       │   ├── partitions/        # Partitioning strategies
│                       │   └── io_managers/       # Custom I/O managers
│                       └── repo.py                # Dagster definitions
├── docs/                                          # Documentation
├── docker-compose.yml                             # Container orchestration
├── Dockerfile                                     # Application container
└── README.md                                      # This file
```

## 🔧 Configuration

### Environment Variables

```bash
# Database Configuration
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=f1_data
POSTGRES_USER=f1user
POSTGRES_PASSWORD=f1password

# Dagster Configuration
DAGSTER_HOME=/opt/dagster/storage
```

### Partitioning Strategies

- **Yearly Partitions**: `yearly_partitions` - For season-based data
- **Monthly Partitions**: `monthly_partitions` - For regular time-series data
- **Static Partitions**: `reference_partitions` - For reference data

### Scheduling Options

- **Weekly Schedules**: Process current data weekly during active seasons
- **Monthly Schedules**: Refresh reference data monthly
- **Annual Schedules**: Initialize new season data yearly

## 📊 Data Pipeline Examples

### F1 Data Pipeline

```python
@asset(
    compute_kind="dlt",
    partitions_def=yearly_partitions,
    group_name="f1_bronze_yearly"
)
def f1_races(context: AssetExecutionContext) -> List[Dict[str, Any]]:
    """Extract F1 race data from Ergast API."""
    # Implementation details...
```

### Job Definitions

```python
f1_race_and_laps_job = define_asset_job(
    name="f1_race_and_laps_job",
    selection=AssetSelection.assets(f1_races, f1_laps),
    description="Extract F1 races and lap data"
)
```

## 🛠️ Development

### Adding New Data Sources

1. Create a source definition in `sources/`
2. Define corresponding assets in `assets/`
3. Configure partitioning and schedules
4. Add to job definitions in `repo.py`

### Custom I/O Managers

Implement custom I/O managers for complex data passing:

```python
class CustomIOManager(IOManager):
    def handle_output(self, context, obj):
        # Store output logic
        pass
    
    def load_input(self, context):
        # Load input logic
        pass
```

## 📈 Monitoring & Operations

### Dagster UI Features

- **Asset Lineage**: Visual representation of data dependencies
- **Run History**: Detailed execution logs and metrics
- **Partition Status**: Track completion across time partitions
- **Schedule Management**: Enable/disable automated runs

### Health Checks

```bash
# Check service status
docker compose ps

# View logs
docker compose logs dagster
docker compose logs postgres

# Access Dagster shell
docker compose exec dagster dagster --help
```

## 🔗 API Integration

### Rate Limiting

Built-in rate limiting for external APIs:

```python
# Configurable delays between requests
time.sleep(10)  # 10-second delay for API compliance
```

### Error Handling

Comprehensive error handling with retries and logging:

```python
try:
    pipeline.run(source)
except Exception as e:
    context.log.error(f"Pipeline failed: {e}")
    raise
```

## 📚 Documentation

- `architecture/` - System design and component diagrams
- `guides/` - User and developer guides  
- `api/` - API documentation and references

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Submit a pull request

## 📄 License

[License information to be added]

## 🆘 Support

For issues and questions:

- Create an issue in the repository
- Check the documentation in docs
- Review the Dagster UI for pipeline status

---

**Data Platform Agent Builder** - Building intelligent, scalable data pipelines with ease.