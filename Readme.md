# ClinicalTrials.gov ELT Pipeline

*This is an ETL - ELT hybrid pipeline built with production principles as a guide: fault tolerance, and separation of concerns.*

The pipeline ingests clinical trial data from ClinicalTrials.gov API, stages it in PostgreSQL, and transforms it using dbt into a dimensional model for analytics.

**Stack:** Python, PostgreSQL, dbt, Docker,

## Architecture Overview
![Alt text](documentation/architecture.png)


[See full architecture](documentation/ARCHITECTURE.md)
---

## Prerequisites

- Docker and Docker Compose installed
- git

## Setup and Configuration



### 1. Clone the Repository
```
git clone https://github.com/DesayoEm/clinical_trial_pipeline
cd clinical_trial_pipeline
```


Create a `.env` file in the root directory with the following variables:

```env
# API Configuration
BASE_URL=https://clinicaltrials.gov/api/v2/studies?pageSize=100
PAGES_BASE_URL=https://clinicaltrials.gov/api/v2/studies?pageSize=100&pageToken=

# Database Configuration 
DB_HOST=pipeline_db
DB_PORT=5432
DB_NAME=clinical_trials
DB_USER=postgres
DB_PASSWORD=your_secure_password
DATABASE_URL=postgresql+psycopg2://postgres:your_secure_password@pipeline_db:5432/clinical_trials

#storage Paths (Docker paths)
SHARD_STORAGE_DIR=/app/data/shards
COMPACTED_STORAGE_DIR=/app/data/compacted
STATE_MGT_DIR=/app/states
DBT_DIR=/app/dbt_studies

# Docker Compose
COMPOSE_FILE=docker-compose.yml
```
**Note:** For running outside Docker, update the storage paths to your local directories, anf use localhost for the db host
##  Running the Pipeline

###  One-time Manual Run
```
# Build and run all services once
docker-compose up --build

# Stop when done
docker-compose down
```
###   Enable Daily Automated Runs
```
# Start services with cron enabled (etl daily at 12 AM, dbt at 1 AM)
docker-compose up -d

# Check logs
docker logs -f ct-etl-pipeline

# Stop automated runs
docker-compose down
```

### Cleaning Up
```
docker-compose down
docker-compose down -v
```


