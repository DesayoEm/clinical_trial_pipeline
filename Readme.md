# ClinicalTrials.gov ELT Pipeline

*This is a locally managed ETL - ELT hybrid pipeline built with production principles as a guide: fault tolerance, and separation of concerns.*

The pipeline ingests clinical trial data from ClinicalTrials.gov API, stages it in PostgreSQL, and transforms it using dbt into a dimensional model for analytics.

**Stack:** Python, PostgreSQL, dbt, Docker,

## Architecture Overview
![Alt text](documentation/architecture.png)


[See full architecture](documentation/ARCHITECTURE.md)
---

## Configuration

Create a `.env` file in the root directory with the following variables:

```env
#API

BASE_URL=https://clinicaltrials.gov/api/v2/studies?pageSize=100
PAGES_BASE_URL=https://clinicaltrials.gov/api/v2/studies?pageSize=100&pageToken=

#DATABASE

DB_HOST=pipeline_db
DB_PORT=5432
DB_NAME=clinical_trials
DB_USER=your username
DB_PASSWORD=your_password

#STORAGE
DATABASE_URL='postgresql+psycopg2://username:password@pipeline_db:5432/clinical_trials'
SHARD_STORAGE_DIR=your/parquet/shards/directory
COMPACTED_STORAGE_DIR=your/compacted/parquet/directory
STATE_MGT_DIR=your/state/management/directory
DBT_DIR=your/dbt/project/directory
COMPOSE_FILE=docker-compose.yml
```

## Setup

### Clone the Repository

```bash
git clone <your-repo-url>
cd clinical_trial-pipeline
```


##  Running the Pipeline

```bash
docker-compose build etl

docker-compose up -d pipeline_db

docker-compose ps studies

docker-compose run --rm etl

```

### Cleaning Up

```bash
docker-compose down

docker-compose down -v

docker system prune -f
```


