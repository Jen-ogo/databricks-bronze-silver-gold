# databricks-bronze-silver-gold

A lightweight, version-controlled workspace for a geo/EV-charging data platform built on **Databricks + ADLS Gen2 + Snowflake**, following a **Bronze → Silver → Gold** approach.

This repo is intentionally **pragmatic**:
- keep **dev SQL / notebooks** close to the code,
- validate logic quickly,
- then migrate/encode finalized transformations into **dbt models** (source of truth for PROD transformations).

---

## Why this repo exists

We need a single place to store:
- **Databricks PROD notebooks / jobs logic** (batch + streaming),
- **Snowflake DDL / ingest assets** (stages/streams/tasks/procedures),
- **Silver/Gold SQL prototypes** used to validate datasets and geometry before formalizing into dbt.

> **Important:** Many Snowflake/DWH transformations will be implemented (or already exist) in **dbt**.  
> The SQL here is **preliminary DEV scaffolding** for correctness checks, iteration, and fast debugging.

---

## Architecture (high level)

**ADLS Gen2** is the raw storage layer for Parquet outputs (OSM / EUROSTAT / GISCO).  
Then we ingest into **Snowflake BRONZE** (WKT kept for debug), and transform into **SILVER/GOLD** either in:
- **Snowflake SQL** (for ingestion / governance / analytics use-cases),
- and/or **Databricks** (for heavy compute, feature engineering, streaming).

**Bronze**
- raw ingest from ADLS Parquet
- `SOURCE_FILE` tracked for idempotent reloads
- geometry stored as `GEOM_WKT` string for validation/debug

**Silver**
- canonical cleaned datasets (typing, null normalizations, dedup rules)
- geo casting / validation (e.g., WKT → GEOGRAPHY) usually happens here

**Gold**
- analytical / ML-ready feature tables (e.g., H3 features, regional aggregates)

```md
## Repo layout

### Databricks
```text

databricks_bronze_silver_gold/
  bronze_ingest.py            # batch ingest prototype / helpers
  SILVER_databricks.sql       # silver transforms (prototype)
  GOLD_databricks.sql         # gold features (prototype)
  
snowflake_bronze_silver_gold/
  bronze_stage_ingest_ddl.sql # ADLS Parquet -> BRONZE ingest infra (stages/streams/tasks/procs)
  SILVER_snowflake.sql        # silver transforms (prototype)
  GOLD_snowflake.sql          # gold transforms/features (prototype)

## Snowflake: what’s inside (DEV scaffolding)

The Snowflake folder provides a canonical bootstrap for ingesting Parquet from ADLS Gen2 into BRONZE using:

- External stages (separate roots for **OSM / EUROSTAT / GISCO**)
- Stage `DIRECTORY` + `DIRECTORY STREAM` for change detection
- One APPLY procedure per BRONZE table (reload by `SOURCE_FILE`)
- Tasks per stage:
  1) `ALTER STAGE ... REFRESH`
  2) `CALL APPLY_*()` (apply changes based on directory stream / directory scan fallback)

### Notes about DDL vs dbt

- **Transformations (SILVER/GOLD)**: the final version should live in **dbt** (or already does).
- **Ingest infrastructure (stages/streams/tasks/procs)**: typically managed as Snowflake “ops” SQL; can remain here or be orchestrated via dbt/CI depending on how the platform is organized.
- The SQL in this repo is primarily to:
  - iterate quickly,
  - validate data shape & geometry,
  - prevent regressions before promoting logic to dbt.

---

## Databricks: what’s inside (PROD notebooks & jobs)

This repo will store **production-grade** Databricks assets:
- notebooks/scripts for batch ETL,
- streaming pipelines (e.g., EventHub / Auto Loader / Delta),
- operational job logic (schedules, merges, checkpoints),
- “gold” feature engineering jobs (H3-based aggregation, scoring prep, etc.).

> Goal: keep the actual operational Databricks logic versioned, reviewable, and reproducible.

---

## How to run (Snowflake quickstart)

1) Run bootstrap / ingest DDL:
```sql
-- snowflake_bronze_silver_gold/bronze_stage_ingest_ddl.sql


---

## Repo layout
