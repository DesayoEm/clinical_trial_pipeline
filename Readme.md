# ClinicalTrials.gov ELT Pipeline

*This is a locally managed ETL - ELT hybrid pipeline was built with production principles in mind: fault tolerance, and separation of concerns.*

The pipeline ingests clinical trial data from ClinicalTrials.gov API, stages it in PostgreSQL, and transforms it using dbt into a dimensional model for analytics.

*I hope you have fun going through it as much as I had building it.*

**Stack:** Python, PostgreSQL, dbt, Docker, lots of Coffee and Sugar.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    EXTRACTION LAYER                     │
│  API → Parquet Shards (crash-resilient, stateful)       │
│               Rate Limited: 50 req/min                  │
│                         
                       COMPACTION                         │
│        Parquet Shards -> Single Compacted File           │
│           (Storage optimization for loading)            │
└─────────────────────────────────────────────────────────┘
                           |
                           V
┌─────────────────────────────────────────────────────────┐
│             TRANSFORMATION/LOADING LAYER                │
│    Parquet → Pandas (flatten) -> Postgres Staging       │
│                 (Structured tables)                     │
└─────────────────────────────────────────────────────────┘
                           |
                           V
┌─────────────────────────────────────────────────────────┐
│             FINAL TRANSFORMATION LAYER                  │
│            dbt models: facts & dimensions               │
└─────────────────────────────────────────────────────────┘
```
## Why Hybrid ETL-ELT?

**Challenge:** ClinicalTrials.gov data has deeply nested JSON with no natural keys for dimensional entities (sponsors, interventions, sites). Once flattened, parent-child relationships are permanently lost.

**Solution:** 
- **Extract + Transform (Python):** Parse nested structures, generate surrogate keys, preserve relationships
- **Load (Postgres):** Store structured dimensional tables
- **Transform (dbt):** Business logic, aggregations, snapshots

**Why not pure ELT?** Loading raw nested JSON to Postgres and parsing in SQL would:
- Create unreadable dbt models with complex JSON path navigation
- Lose referential integrity (can't link studies to interventions after flattening)
- Perform poorly (SQL JSON parsing is slower than Python wrangling)

---
## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    EXTRACTION LAYER                     │
│  API → Parquet Shards (crash-resilient, stateful)       │
│               Rate Limited: 50 req/min                  │
│                         
                       COMPACTION                         │
│        Parquet Shards -> Single Compacted File           │
│           (Storage optimization for loading)            │
└─────────────────────────────────────────────────────────┘
                           |
                           V
┌─────────────────────────────────────────────────────────┐
│             TRANSFORMATION/LOADING LAYER                │
│    Parquet → Pandas (flatten) -> Postgres Staging       │
│                 (Structured tables)                     │
└─────────────────────────────────────────────────────────┘
                           |
                           V
┌─────────────────────────────────────────────────────────┐
│             FINAL TRANSFORMATION LAYER                  │
│            dbt models: facts & dimensions               │
└─────────────────────────────────────────────────────────┘
```

## Key Design Decisions
### Crash resistant extraction with State Persistence
ClinicalTrials.gov uses token-based pagination. You can't jump to a page without extracting the token from the previous page, and the same token always returns the same data

On restart after a crash, the program reads the last token and last loaded page, and resume extraction without re-calling previous API pages.

**Implementation:** I Initialized the `Extractor` class with `last_saved_page`, `last_saved_token`, and `next_page_url`. I also persisted state in `states/last_extraction_result.py`, 
`states/last_shard_path.py`, and `last_token.py`

File writes are unconventional, but I think it makes sense for a locally built and managed stack.

### Crash Recovery Flow

1. **On Start:** The script checks `extraction_result.py`
   - If the state of the last extraction is `SUCCESS`: Then it starts a fresh extraction (page 0)
   - `FAILURE` or `IN PROGRESS`: Resume from last saved page in current shard as the last extraction that ran was interrupted

2. **During Extraction:** 
   - Write `IN PROGRESS` status before each request
   - Save JSON data as a parquet file immediately after a successful API call
   - Update `last_token.py` with next page token

3. **On Completion:** 
   - Write `SUCCESS` status
   - Trigger compaction of parquet shards into a single file

**Why This Works:**
- Status file distinguishes clean shutdown from crash
- Token persistence enables exact resume point



### Rate Limit Handling With Sliding Window Strategy

**Rationale:**
ClinicalTrials.gov limits requests to 50 /minute/ IP. A Rate limit helper method tracks the last 50 requests and sleeps only when necessary.

`time.sleep()` every one or two seconds doesn't account for variable network latency between requests, and wastes time when requests are fast.
Sliding window logic maximizes throughput without violating API limits. 

**Tradeoff:** A teeny tiny bit more complex wait logic but significantly faster and more dependable. The extractor only needs to wait when a rate limit is close to being exceeded, and not every 1 or 2 seconds


### Persisted data locally (API → Parquet → Postgres), not direct streaming
**Rationale:**
- **Fault Tolerance:** If the pipeline crashes while running, local file storage enables replay without re-extracting from API.
- **Auditability:** Files provide immutable snapshot. If transformation logic has bugs, or needs to evolve, it can be reloaded from local files.

The JSON data is deeply nested, and most of the entities in the data set do not have natural keys. Due to the nature of the JSON, putting it directly into Postgres will make it highly difficult to manage and query.

Also, direct API → Postgres streaming couples extraction and loading. Failures will require full re-extraction. 

**Tradeoff:** Extra disk space usage and one additional read operation. Negligible cost for significant operational benefits.


### Parquet for Raw Storage
**Decision:** Store extracted API responses as Parquet files (not JSON).

**Rationale:**
- Parquet files are 5-10x smaller than raw JSON , and Columnar format enables selective column reads

**Alternative Considered:** Gzipped JSON  
**Rejection reason:** While simpler, gzipped JSON requires full decompression and parsing for any operation. Parquet's columnar structure allows predicate pushdowns, and optimizes loading into Postgres.

**Tradeoff:** Slightly more complex write logic during extraction, but significantly faster reads during loading.


### Separate Shards and Compaction for Parquet files
I wrote to small Parquet files during extraction (1.parquet, 2.parquet...), then compact into a single file before loading.

**Rationale:**
- **Crash Resilience:** Each page is saved immediately. If extraction fails at page 50, pages 1-49 are safely on disk. On restart, script detects existing files and continues from last saved page.
- **Storage Efficiency:** After extraction is complete, compaction merges small files into one, reducing overhead and simplifying Postgres loading.

**Alternative Considered:** Write directly to one large Parquet file  
**Rejection reason:** Parquet append operations are complex. Also,  the script crashes mid-extraction, the entire file could be corrupted, requiring full re-extraction from the API.

**Tradeoff:** Extra compaction step adds some latency to the pipeline(depending on the number of records), but provides fault tolerance worth more than the time cost.


### Schema Flattened During load and not in dbt

**Rationale:*
- **Separation of Concerns:** Python(Pandas) is better suited for data wrangling (nested JSON handling, type coercion) than SQL. 
- **Performance:** Parsing JSON in SQL (`data->'key'->'subkey'`) is slower than pre-flattening in Python.
- **Testability:** Flattened columns enable simple dbt tests (`not_null`, `unique`) on specific fields.

**Alternative Considered:** Load raw JSON as JSONB column, flatten in dbt staging models  
**Rejection reason:**  violates best practices. The Load step should produce "analysis-ready" data. Pushing structure complexity to dbt makes models unreadable and hard to test.

**Tradeoff:** More complex Python loading logic, but cleaner dbt models and faster query performance.


## Transformation Layer (dbt)

While structural transformations happen in Python, dbt handles:
**Incremental Models:**
- Monthly study snapshots 
- Only processes new/changed records

**Data Quality:**
- Uniqueness tests on surrogate keys
- Not-null tests on critical fields
- Referential integrity checks across tables

**Documentation:**
- Auto-generated lineage graphs
- Column-level descriptions
- Model dependencies

## Setup and Config

### Clone the Repository

```bash
git clone <your-repo-url>
cd netflix-etl-pipeline
```

### Create Environment File

Create a `.env` file in the root directory with the following variables:

```env
# db Configuration
DB_HOST=local_host
DB_PORT=5432
DB_NAME=netflix
DB_USER=your username
DB_URL=postgresql+psycopg2://username:password@netflix:5432/netflix
DB_PASSWORD=your_password
CSV_FILE_PATH=./netflix_titles.csv
COMPOSE_FILE=docker-compose.yml
```

##  Running the Pipeline

```bash
docker-compose build netflix-etl

docker-compose up -d netflix

docker-compose ps netflix

docker-compose run --rm netflix-etl

```

### Cleaning Up

```bash
docker-compose down

docker-compose down -v

docker system prune -f
```


