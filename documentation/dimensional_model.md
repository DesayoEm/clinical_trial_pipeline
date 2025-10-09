# Clinical Trials Data Warehouse - Dimensional Model

## Overview

This dimensional model follows the **star schema** design pattern, optimized for analytical queries on clinical trial data from ClinicalTrials.gov. 

## Dimension Tables

### dim_study
**Purpose:** Core study information - the primary dimension for all analyses.

**Grain:** One row per clinical trial study.

**Attributes:**
- `study_key` (PK): Surrogate key (MD5 hash)
- `nct_id` (NK): Natural key - ClinicalTrials.gov identifier (e.g., NCT01153035)
- `brief_title`: Short study title
- `official_title`: Full official study title
- `acronym`: Study acronym (e.g., ABLATE)
- `brief_summary`: Study objective summary
- `detailed_description`: Comprehensive study description
- `overall_status`: Current status (RECRUITING, COMPLETED, TERMINATED, etc.)
- `why_stopped`: Reason for early termination (if applicable)
- `study_type`: INTERVENTIONAL or OBSERVATIONAL
- `allocation`: Randomization method (RANDOMIZED, NON_RANDOMIZED, NA)
- `intervention_model`: Study design (PARALLEL, SINGLE_GROUP, etc.)
- `primary_purpose`: TREATMENT, PREVENTION, DIAGNOSTIC, etc.
- `masking`: Blinding approach (NONE, SINGLE, DOUBLE, TRIPLE, QUADRUPLE)
- `patient_registry`: Boolean - is this a patient registry study
- `enrollment_count`: Target/actual number of participants
- `enrollment_category`: Derived category (Small <100, Medium 100-999, Large ≥1000)
- `healthy_volunteers`: Boolean - accepts healthy volunteers
- `sex`: ALL, FEMALE, MALE
- `minimum_age_years`: Minimum age in years
- `maximum_age_years`: Maximum age in years
- `age_group`: Derived category (Pediatric, Adults, Seniors, All Ages)
- `start_date`: Study start date (YYYY-MM format)
- `completion_date`: Expected/actual completion date
- `planned_duration_months`: Calculated study duration
- `has_expanded_access`: Boolean - expanded access available
- `has_dmc`: Boolean - Data Monitoring Committee oversight
- `source_last_updated_date`: Last update from source system
- `etl_etl_created_at`: When record was created in warehouse
- `dbt_updated_at`: Last update timestamp


### dim_Sponsor
**Purpose:** Organizations funding or conducting clinical trials.

**Grain:** One row per unique sponsor organization.

**Attributes:**
- `sponsor_key` (PK): Surrogate key
- `sponsor_name` (NK): Organization name (e.g., "University of Arkansas")
- `sponsor_class`: Organization type
- `etl_created_at`: Record creation timestamp


### dim_condition
**Purpose:** Medical conditions being studied.

**Grain:** One row per unique medical condition.

**Attributes:**
- `condition_key` (PK): Surrogate key
- `condition_name` (NK): Condition description (e.g., "Breast Cancer", "COPD")

---

### dim_intervention
**Purpose:** Treatments, medications, or procedures being tested.

**Grain:** One row per unique intervention.

**Attributes:**
- `intervention_key` (PK): Surrogate key
- `intervention_name` (NK): Intervention name (e.g., "Radiofrequency Ablation")
- `intervention_type`: Category of intervention
  - `DRUG`: Pharmaceutical medications
  - `DEVICE`: Medical devices
  - `BIOLOGICAL`: Biological/vaccine products
  - `PROCEDURE`: Surgical or medical procedures
  - `BEHAVIORAL`: Behavioral interventions
  - `OTHER`: Other intervention types
- `intervention_description`: Detailed description of the intervention


### dim_site
**Purpose:** Physical locations where studies are conducted.

**Grain:** One row per unique facility/location combination.

**Attributes:**
- `site_key` (PK): Surrogate key
- `facility_name`: Name of research facility/hospital
- `city`: City name
- `state`: State/province (if applicable)
- `zip`: Postal code
- `country`: Country name
- `latitude`: Geographic coordinate
- `longitude`: Geographic coordinate

**Geographic Analysis:**
- Enables spatial analysis of clinical trial distribution
- Can identify underserved regions
- Supports distance-based site selection


### dim_date
**Purpose:** Date dimension for time-based analysis.

**Grain:** One row per calendar date.

**Attributes:**
- `date_key` (PK): Integer surrogate key (YYYYMMDD format)
- `full_date`: Actual date
- `year`: Calendar year
- `quarter`: Calendar quarter (1-4)
- `month`: Month number (1-12)
- `month_name`: Month name (January, February, etc.)
- `week`: ISO week number
- `day_of_month`: Day number (1-31)
- `day_of_week`: Day number (1=Monday, 7=Sunday)
- `day_name`: Day name (Monday, Tuesday, etc.)
- `is_weekend`: Boolean - Saturday or Sunday
- `is_holiday`: Boolean - designated holiday (customizable)

**Usage:**
- Time-series analysis of study starts/completions
- Trend analysis over time


## Fact Tables

### Fact_Study_Sponsor
**Purpose:** Links studies to their funding/conducting organizations.

**Grain:** One row per study-sponsor relationship.

**Type:** Factless fact table (no numeric measures, pure relationships)

**Attributes:**
- `study_sponsor_key` (PK): Surrogate key
- `study_key` (FK): Links to dim_Study
- `sponsor_key` (FK): Links to dim_Sponsor
- `is_lead`: Boolean - true if this is the lead sponsor
- `is_collaborator`: Boolean - true if this is a collaborating sponsor

**Rules**
- Each study has exactly one lead sponsor
- A study may have zero or more collaborators



### Fact_Study_Condition
**Purpose:** Links studies to the medical conditions being researched.

**Grain:** One row per study-condition relationship.

**Type:** Factless fact table

**Attributes:**
- `study_condition_key` (PK): Surrogate key
- `study_key` (FK): Links to dim_Study
- `condition_key` (FK): Links to dim_Condition

**Rules**
- A study must have at least one condition
- A study can target multiple conditions
- The same condition can be studied by many trials


### Fact_Study_Intervention
**Purpose:** Links studies to the interventions being tested.

**Grain:** One row per study-intervention relationship.

**Type:** Factless fact table

**Attributes:**
- `study_intervention_key` (PK): Surrogate key
- `study_key` (FK): Links to dim_Study
- `intervention_key` (FK): Links to dim_Intervention

**Rules**
- Interventional studies must have at least one intervention
- Observational studies may have zero interventions
- A study can test multiple interventions (combination therapy)



### Fact_Study_Site
**Purpose:** Links studies to their research locations.

**Grain:** One row per study-site relationship.

**Type:** Factless fact table

**Attributes:**
- `study_site_key` (PK): Surrogate key
- `study_key` (FK): Links to dim_Study
- `site_key` (FK): Links to dim_Site

**Rules**
- Single-site studies have one location
- Multi-site studies have multiple locations
- Same facility can conduct many studies


### Fact_Study_Snapshot
**Purpose:** Point-in-time snapshot of study metrics, captured monthly.

**Grain:** One row per study per snapshot date.

**Type:** Periodic snapshot fact table (accumulating metrics over time)

**Attributes:**
- `snapshot_key` (PK): Surrogate key
- `snapshot_date_key` (FK): Links to dim_Date
- `study_key` (FK): Links to dim_Study

**Snapshot Metrics:**
- `overall_status`: Status at time of snapshot
- `enrollment_count`: Enrollment at time of snapshot
- `sponsor_count`: Number of sponsors
- `condition_count`: Number of conditions being studied
- `intervention_count`: Number of interventions
- `site_count`: Number of active sites
- `country_count`: Number of countries with sites
- `complexity_score`: Calculated metric (weighted average of sites, interventions, collaborators)
- 
**Purpose:**
- Track how studies evolve over time
- Identify trends in enrollment, site activation
- Historical reporting and analysis


### Data Quality Checks (dbt tests)
```yaml
# Dimension tests
- unique and not_null on all primary keys
- unique on natural keys (nct_id, sponsor_name, etc.)
- accepted_values for categorical fields (study_type, overall_status)

# Fact table tests
- relationships to dimension tables
- unique combination of foreign keys where applicable
```

### Refresh Schedule
- **Dimensions**: Daily full refresh (small tables, fast)
- **Facts (non-snapshot)**: Daily incremental (new relationships only)
- **Fact_Study_Snapshot**: Monthly full snapshot on 1st of month

### Data Lineage
```
ClinicalTrials.gov API
  → Parquet files (raw extract)
  → PostgreSQL staging schema (Python ETL)
  → dbt models (transformations)
  → PostgreSQL analytics schema (dimensional model)
```

