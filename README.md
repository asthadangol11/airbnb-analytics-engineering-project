# Airbnb & Census Data Pipeline — dbt + Airflow + Postgres + GCP

An end-to-end ELT pipeline following the **Medallion Architecture (Bronze → Silver → Gold)**.

## Tech Stack
- **Airflow** Apache Airflow for orchestration
- **dbt** dbt Cloud for transformations and version control
- **Postgres** Postgres + DBeaver as the warehouse and SQL interface
- **GCP Cloud Storage** (GCP) for centralized data storage

### It follows a Medallion architecture (Bronze → Silver → Gold) ensuring reproducible, traceable, and analytics-ready data for downstream use.


## Objectives
- **Data Ingestion (Airflow → Bronze)**
    - Ingest** monthly Airbnb CSVs, Census, and LGA mapping data into Postgres.
    - Maintain raw structure for auditability and historical traceability.
- **Transformation & Modeling (dbt → Silver & Gold)**
    - Clean, standardize, and normalize Airbnb + Census data.
    - Apply SCD Type 2 snapshots for dynamic entities (hosts, listings).
    - Design star schema (fact + dimension tables) for analytics
- **Datamarts & Ad-hoc Analysis**
    - Build curated datamart views for property, host, and neighbourhood performance.
    -  Derive KPIs such as revenue, superhost rate, and mortgage-to-revenue coverage
- **Sequential Monthly Processing** 
    - Chronologically process monthly data via Airflow DAGs to ensure consistency.
- **Business Insights**
    - Correlate demographic indicators with Airbnb revenue to assess performance by LGA.

## Pipeline Flow
1. Raw Airbnb data → GCS (Bronze)
2. Airflow DAG triggers dbt transformations
3. dbt models build Silver & Gold tables in Postgres
4. Gold datamarts serve insights for analysis & BI

## Tech Stack
- Programming: Python, SQL, Jinja (dbt)
- Data Orchestration: Apache Airflow
- Transformation & Modeling: dbt Cloud
- Warehouse: PostgreSQL (via DBeaver)
- Cloud Platform: Google Cloud Platform (GCS, Composer, Cloud SQL)
- Visualization: Power BI / Tableau (for future insights)

##  Repo Structure
- `airflow/` – DAGs 
- `dbt/` – models, snapshots, and macros
- `sql_scripts/` – setup SQL and queries for table creation and adhoc analysis



