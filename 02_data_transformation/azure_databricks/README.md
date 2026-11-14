# Azure Databricks | Oncology Analytics Transformation

A **Databricks-native lakehouse** for oncology commercial analytics, built using **Azure Data Lake Storage Gen2, Azure Databricks, Unity Catalog, Delta Lake, PySpark, Spark SQL, and Databricks Asset Bundles (DABs)**.

This project replaces the earlier Airflow–Snowflake–dbt transformation architecture with native Databricks ingestion, processing, and orchestration.

## Architecture

**Data Flow**

```text
ADLS Gen2 (Landing)
        │
        ▼
Databricks Auto Loader
        │
        ▼
BRONZE (Raw Delta Tables)
        │
        ▼
SILVER
Staging → Bridges → Dimensions → Facts → IDL
        │
        ▼
GOLD
Intermediate KPIs → Aggregates
        │
        ▼
Power BI / Analytics
```

**Unity Catalog:** `onco_pharma_analytics`

| Layer | Schema | Processing |
|---|---|---|
| Bronze | `bronze` | Incremental file ingestion using Auto Loader |
| Silver | `silver` | Standardization, relationships, SCD2 dimensions, incremental facts and IDL |
| Gold | `gold` | Business KPIs and reporting aggregates |

All persisted analytical tables use **Delta Lake**.

## Project Structure

```text
azure_databricks/
├── databricks.yml
├── resources/
│   ├── 00_main_pipeline_job.yml
│   ├── 01_bronze_layer_ingestion_job.yml
│   ├── 02_silver_staging_layer_job.yml
│   ├── 03_silver_bridge_layer_job.yml
│   ├── 04_silver_dimension_layer_job.yml
│   ├── 05_silver_fact_layer_job.yml
│   ├── 06_silver_idl_layer_job.yml
│   ├── 07_gold_int_layer_job.yml
│   └── 08_gold_aggregate_layer_job.yml
├── bronze/
│   └── bronze_tables_ingestion.py
├── config/
│   └── stg_config.json
├── silver/
│   ├── staging/
│   ├── bridge/
│   ├── dimension/
│   ├── fact/
│   └── intermediate_data/
├── gold/
│   ├── intermediate_kpi/
│   └── aggregate/
└── utils/
    └── get_max_reporting_date.sql
```

## Data Processing

| Component | Models | Implementation |
|---|---:|---|
| Bronze | 1 ingestion entrypoint | Auto Loader, incremental file discovery, raw Delta tables |
| Silver Staging | 28 mappings | Config-driven PySpark cleansing and type normalization |
| Silver Bridges | 3 | Entity relationships and mappings |
| Silver Dimensions | 17 | Conformed dimensions and selected SCD Type 2 history |
| Silver Facts | 12 | Incremental Delta MERGE / upsert patterns |
| Silver IDL | 7 | Integrated analytical datasets and patient journeys |
| Gold Intermediate | 10 | Business KPI calculations |
| Gold Aggregates | 5 | Reporting-ready analytical marts |

**Processing highlights:**
- **Bronze:** Preserves source data with persistent Auto Loader schema and checkpoint tracking.
- **Staging:** Applies reusable transformation rules from `stg_framework.py` and `stg_config.json`.
- **Dimensions:** Maintains historical changes using SCD2 validity columns and row hashes where applicable.
- **Facts:** Handles incoming inserts and updates through Delta MERGE.
- **Gold:** Produces HCP, patient, product and territory analytics for downstream reporting.

*Counts represent repository definitions, not verified deployed tables.*

## Orchestration

**Databricks Jobs**, defined in `resources/`, orchestrate the complete pipeline through Databricks Asset Bundles.

```text
Bronze Ingestion
      ↓
Silver Staging
      ↓
Silver Bridges
      ↓
Silver Dimensions
      ↓
Silver Facts
      ↓
Silver IDL
      ↓
Gold Intermediate KPIs
      ↓
Gold Aggregates
```

The main pipeline coordinates eight layer jobs using `run_job_task` dependencies. Layer jobs include model-level dependencies and completion gates.

Staging supports parallel table processing using **For each** tasks with concurrency 4.

## Deployment

### Prerequisites

- Azure Databricks workspace with Unity Catalog enabled.
- ADLS Gen2 landing storage and authorized Auto Loader checkpoint/schema locations.
- Appropriate storage permissions, Unity Catalog privileges and supported Databricks job compute.
- Databricks CLI authenticated to the target workspace.
- Validated bundle configuration, model paths and catalog/schema references.

### Commands

Run from `02_data_transformation/azure_databricks`:

```bash
# Authenticate
databricks auth login --host <workspace-url>

# Validate bundle
databricks bundle validate -t dev

# Deploy resources
databricks bundle deploy -t dev

# Execute pipeline
databricks bundle run main_pipeline_job -t dev
```

The current bundle defines a **development target only**. Production deployment and GitHub Actions CI/CD are not included in this repository.

## Data Quality & Validation

Recommended layer-level checks include:

- **Bronze:** File ingestion counts and source reconciliation.
- **Staging:** Schema validation, null checks and business-key uniqueness.
- **Dimensions:** SCD2 current-record and validity-period validation.
- **Facts:** Merge-key uniqueness, referential integrity and reconciliation.
- **Gold:** KPI consistency and aggregate validation.

These checks are recommended; a complete automated testing suite is not included.

## Deployment Status & Considerations

**Status:** Repository definitions are available; successful end-to-end deployment and execution have not yet been verified.

Before executing the complete pipeline:

1. Resolve mismatches between configured notebook paths (`.ipynb`) and checked-in `.py`/`.sql` files; use supported task types and executable formats.
2. Verify ADLS access permissions for landing, checkpoints and Auto Loader schema tracking.
3. Configure the correct workspace host, catalog and storage paths.
4. Validate model dependencies, incremental MERGE keys and compute configuration.
5. Add executable data-quality checks where appropriate; completion gates alone do not validate data.

## Technology Stack

**Cloud:** Microsoft Azure, ADLS Gen2  
**Processing:** Azure Databricks, PySpark, Spark SQL  
**Storage:** Delta Lake  
**Governance:** Unity Catalog  
**Ingestion:** Databricks Auto Loader  
**Orchestration:** Databricks Jobs  
**Deployment:** Databricks Asset Bundles, Databricks CLI  
**Version Control:** Git, GitHub  
**Reporting Target:** Power BI

---

**Related:** [Main Project README](../../README.md) | [Architecture Diagram](../../docs/architecture.png)
