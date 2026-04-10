# Modern Data Engineering Stack — Production-Safe — April 2026

> **PRODUCTION-SAFE** — Battle-tested, widely adopted, low-risk.

```
dlt → S3 (Iceberg) → Glue Catalog → dbt + Athena → S3 (Iceberg) → Cube.dev → Consumers
```

Orchestrated by **Dagster**. Quality checked by **dbt tests + Soda**.

---

## 1. Ingestion — dlt (Data Load Tool)

**What:** Python-native ELT framework that extracts data from APIs, databases, and files, then loads it directly into your data lake as Iceberg tables.

**Why dlt:**
- No separate infrastructure — it's just a Python library (`pip install dlt`)
- Automatic schema inference and evolution on write
- Built-in Iceberg destination support with Glue Catalog sync
- Handles pagination, rate limiting, retries, incremental loading out of the box
- Version-controlled pipelines — your ingestion is just Python code in Git
- Free and open source — no per-connector licensing like Fivetran

**Code Example:**

```python
import dlt

@dlt.source
def my_api():
    @dlt.resource(write_disposition="merge", primary_key="id")
    def orders():
        yield from paginated_api_call("/orders")
    return orders

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="athena",  # writes Iceberg to S3
    dataset_name="raw"
)
pipeline.run(my_api())
```

**Configuration:**

| Setting | Value |
|---|---|
| S3 Bucket | Your data lake bucket (e.g. `s3://my-datalake/`) |
| Glue Catalog | dlt registers tables automatically in Glue |
| Write Disposition | `append`, `replace`, or `merge` (SCD Type 1/2) |
| Incremental | Supports cursor-based and merge-based incremental loads |
| File Format | Parquet by default (optimal for Iceberg) |
| Parallelism | Configurable worker count for concurrent extraction |
| Sources | 200+ verified: Salesforce, Stripe, PostgreSQL, MySQL, REST APIs, Google Sheets, S3 files, MongoDB, HubSpot, GitHub, Slack, etc. |

---

## 2. Storage — S3 + Apache Iceberg

**What:** All data lives as Parquet files on S3, organized by the Iceberg table format. Iceberg adds a metadata layer that gives you warehouse-like features on cheap object storage.

**Why Iceberg on S3:**
- Storage and compute fully decoupled — pay S3 rates ($0.023/GB/month), not warehouse rates
- ACID transactions — no partial writes or corrupted reads
- Schema evolution — add/rename/drop columns without rewriting data
- Time travel — query data as it existed at any point in time
- Partition evolution — change partitioning strategy without rewriting
- Hidden partitioning — no need to include partition columns in queries
- Engine-agnostic — Athena, Spark, Trino, DuckDB all read the same tables

**S3 Structure:**

```
s3://my-datalake/
  ├── warehouse/
  │   ├── raw/                    # dlt writes here
  │   │   ├── orders/
  │   │   │   ├── metadata/       # Iceberg metadata
  │   │   │   │   ├── v1.metadata.json
  │   │   │   │   ├── snap-001.avro
  │   │   │   │   └── manifest-001.avro
  │   │   │   └── data/           # Actual Parquet files
  │   │   │       ├── part-00001.parquet
  │   │   │       └── part-00002.parquet
  │   │   ├── customers/
  │   │   └── products/
  │   ├── staging/                # dbt staging models
  │   └── marts/                  # dbt final models
  └── _dagster/                   # Dagster metadata
```

**Configuration:**

| Setting | Value |
|---|---|
| S3 Bucket Config | Versioning enabled, lifecycle rules for old Iceberg snapshots |
| Iceberg Format Version | v2 (supports row-level deletes, equality deletes) |
| File Format | Parquet with Snappy compression |
| Partition Strategy | Start unpartitioned, add partitioning when tables grow large |
| Snapshot Expiry | Configure to expire old snapshots (e.g. keep 7 days) |
| Compaction | Schedule regular compaction to merge small files |
| Estimated Cost | ~$23/TB/month for storage + minimal API costs |

---

## 3. Catalog — AWS Glue Catalog

**What:** A managed Hive-compatible metastore that acts as a phone book for your Iceberg tables. When Athena or Spark asks "where is the orders table?", Glue Catalog answers.

**Why Glue Catalog:**
- Fully managed — no servers, no Hive Metastore to operate
- Native AWS integration — Athena, EMR, Spark all use it natively
- Stores database/table/partition metadata
- Cheap — first million objects free, then $1 per 100K objects/month
- dlt and dbt-athena both sync to it automatically

**Glue Catalog Hierarchy:**

```
Account
  └── Database: "raw"
  │     ├── Table: "orders"        → s3://my-datalake/warehouse/raw/orders/
  │     ├── Table: "customers"     → s3://my-datalake/warehouse/raw/customers/
  │     └── Table: "products"      → s3://my-datalake/warehouse/raw/products/
  └── Database: "staging"
  │     ├── Table: "stg_orders"    → s3://my-datalake/warehouse/staging/stg_orders/
  │     └── Table: "stg_customers" → s3://my-datalake/warehouse/staging/stg_customers/
  └── Database: "marts"
        ├── Table: "fct_revenue"   → s3://my-datalake/warehouse/marts/fct_revenue/
        └── Table: "dim_customers" → s3://my-datalake/warehouse/marts/dim_customers/
```

Each table entry contains: S3 location, column names/types, Iceberg metadata pointer, table properties (format, compression, etc.).

**Configuration:**

| Setting | Value |
|---|---|
| IAM Permissions | `glue:GetTable`, `glue:GetDatabase`, `glue:CreateTable`, `glue:UpdateTable` |
| Database Naming | One Glue database per dbt schema (`raw`, `staging`, `marts`) |
| Table Type | `EXTERNAL_TABLE` with `table_type=ICEBERG` |
| Lake Formation | Skip unless you need row/column-level security across teams |
| Crawler | Not needed — dlt and dbt register tables directly |
| Cost | Effectively free for most workloads |

---

## 4. Compute — AWS Athena

**What:** Serverless SQL engine that executes your dbt transformations. It reads Iceberg tables from S3 via Glue Catalog, processes the SQL, and writes results back as new Iceberg tables on S3.

**Why Athena:**
- Fully serverless — no clusters to provision, start, stop, or scale
- Pay per query — $5 per TB scanned (no idle costs)
- Native Iceberg support — reads metadata to skip irrelevant files (predicate pushdown)
- Supports dbt via `dbt-athena-community` adapter
- Supports CTAS (Create Table As Select) for materialized dbt models
- Good for small-to-mid workloads (up to ~TB/day of transforms)

**dbt Project Config:**

```yaml
# profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: athena
      s3_staging_dir: s3://my-datalake/athena-results/
      s3_data_dir: s3://my-datalake/warehouse/
      region_name: eu-north-1
      database: raw
      schema: marts
      work_group: primary
      table_type: iceberg
```

**dbt Model Example:**

```sql
-- marts/fct_revenue.sql
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    table_type='iceberg',
    format='parquet'
  )
}}

SELECT
    o.order_id,
    o.order_date,
    c.customer_segment,
    SUM(o.amount) as revenue
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
{% if is_incremental() %}
WHERE o.order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
GROUP BY 1, 2, 3
```

**Configuration:**

| Setting | Value |
|---|---|
| Workgroup | Create separate workgroups for dbt vs ad-hoc queries |
| Query Result Location | `s3://my-datalake/athena-results/` (temp results) |
| Encryption | SSE-S3 or SSE-KMS for results at rest |
| Cost Control | Set per-query data scan limits in workgroup settings |
| Engine Version | Athena v3 (Trino-based, best Iceberg support) |
| When to Migrate | If monthly bill exceeds ~$500-1000, evaluate EMR Serverless |
| Migration Path | Swap dbt adapter from `athena` to `spark`, everything else stays |

---

## 5. Transformation — dbt

**What:** SQL-based transformation framework. You write SELECT statements, dbt compiles them, orders them by dependency, and executes them against Athena. Results are materialized as new Iceberg tables.

**Why dbt:**
- Industry standard for SQL transformations
- Dependency management — dbt figures out execution order from `ref()` calls
- Incremental models — process only new/changed data
- Built-in testing — not null, unique, relationships, custom tests
- Auto-generated documentation and lineage graphs
- Version-controlled — all models live in Git
- Jinja templating for DRY SQL
- Massive community — 50,000+ teams, huge package ecosystem

**Project Structure:**

```
my_dbt_project/
  ├── dbt_project.yml
  ├── profiles.yml
  ├── models/
  │   ├── staging/
  │   │   ├── _stg_models.yml      # schema + tests
  │   │   ├── stg_orders.sql
  │   │   └── stg_customers.sql
  │   ├── intermediate/
  │   │   └── int_orders_enriched.sql
  │   └── marts/
  │       ├── _marts_models.yml
  │       ├── fct_revenue.sql
  │       └── dim_customers.sql
  ├── tests/
  │   └── assert_revenue_positive.sql
  ├── macros/
  │   └── cents_to_dollars.sql
  └── seeds/
      └── country_codes.csv
```

**Staging Model Example:**

```sql
-- models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),
renamed AS (
    SELECT
        id AS order_id,
        customer_id,
        CAST(created_at AS timestamp) AS order_date,
        amount_cents / 100.0 AS amount,
        status
    FROM source
    WHERE status != 'cancelled'
)
SELECT * FROM renamed
```

**Configuration:**

| Setting | Value |
|---|---|
| Materialization | `view` (staging), `incremental` (facts), `table` (dimensions) |
| dbt Adapter | `dbt-athena-community` (`pip install dbt-athena-community`) |
| Testing | Configure `not_null`, `unique`, `accepted_values`, `relationships` in YAML |
| Naming Convention | `stg_` (staging), `int_` (intermediate), `fct_` (facts), `dim_` (dimensions) |
| Environments | dev (personal schema), staging (CI), prod (production) |
| CI/CD | Run `dbt build` on PR → test in staging schema → merge → deploy to prod |

---

## 6. Orchestration — Dagster

**What:** Orchestrator that schedules and monitors your entire pipeline. Unlike Airflow (task-based), Dagster thinks in assets — each dlt source and dbt model is a data asset with dependencies, freshness expectations, and observability.

**Why Dagster:**
- Asset-based — maps naturally to dlt sources and dbt models
- First-class dbt integration — auto-imports dbt models as Dagster assets
- First-class dlt integration — `dagster-dlt` package
- Built-in asset lineage, freshness policies, and auto-materialization
- Local dev experience is excellent (`dagster dev` command)
- Dagster Cloud available for managed deployment
- Sensors and schedules for event-driven or cron-based runs

**Code Example:**

```python
# assets/ingestion.py
from dagster_dlt import DagsterDltResource, dlt_assets

@dlt_assets(
    dlt_source=my_api(),
    dlt_pipeline=pipeline(
        pipeline_name="my_pipeline",
        destination="athena",
        dataset_name="raw"
    ),
    name="raw_data",
    group_name="ingestion",
)
def raw_data_assets(context, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

# assets/transformation.py
from dagster_dbt import DbtCliResource, dbt_assets
from dagster import AssetExecutionContext

@dbt_assets(manifest=dbt_manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# schedules.py
from dagster import ScheduleDefinition, define_asset_job

daily_refresh = ScheduleDefinition(
    job=define_asset_job("daily_refresh", selection="*"),
    cron_schedule="0 6 * * *",  # 6 AM daily
)
```

**Configuration:**

| Setting | Value |
|---|---|
| Deployment | Dagster Cloud (managed) or self-hosted on ECS/K8s |
| dagster-dlt | `pip install dagster-dlt` (official integration) |
| dagster-dbt | `pip install dagster-dbt` (official integration) |
| Freshness Policies | Define SLAs per asset (e.g. orders must be <6 hours old) |
| Auto-Materialization | Dagster can auto-trigger downstream when upstream refreshes |
| Sensors | Trigger pipelines on S3 file arrival, API webhooks, etc. |
| Partitions | Supports daily/weekly/monthly partitioned assets natively |

---

## 7. Semantic Layer — Cube.dev

**What:** Defines your business metrics (revenue, churn, ARR, etc.) once in code, then exposes them via REST/GraphQL API to any consumer — BI tools, notebooks, LLM agents, internal apps. Single source of truth for metrics.

**Why Cube.dev:**
- Define metrics once, use everywhere — no more conflicting definitions across tools
- Built-in caching and pre-aggregation — fast queries without hitting Athena every time
- REST and GraphQL APIs — any app or LLM can query your metrics
- Supports Athena as a data source natively
- Access control — define who sees what data
- Open source core with managed cloud option

**Schema Example:**

```javascript
// cube/schema/Revenue.js
cube('Revenue', {
  sql_table: 'marts.fct_revenue',

  measures: {
    total_revenue: {
      type: 'sum',
      sql: 'revenue',
      format: 'currency',
    },
    order_count: {
      type: 'count',
    },
    avg_order_value: {
      type: 'number',
      sql: `${total_revenue} / NULLIF(${order_count}, 0)`,
      format: 'currency',
    },
  },

  dimensions: {
    order_date: {
      type: 'time',
      sql: 'order_date',
    },
    customer_segment: {
      type: 'string',
      sql: 'customer_segment',
    },
  },

  pre_aggregations: {
    daily_revenue: {
      measures: [CUBE.total_revenue, CUBE.order_count],
      dimensions: [CUBE.customer_segment],
      time_dimension: CUBE.order_date,
      granularity: 'day',
      refresh_key: {
        every: '1 hour',
      },
    },
  },
});
```

**API Query Example:**

```
GET /cubejs-api/v1/load?query={
  "measures": ["Revenue.total_revenue"],
  "timeDimensions": [{
    "dimension": "Revenue.order_date",
    "granularity": "month"
  }],
  "filters": [{
    "member": "Revenue.customer_segment",
    "operator": "equals",
    "values": ["Enterprise"]
  }]
}
```

**Configuration:**

| Setting | Value |
|---|---|
| Data Source | Athena (configured via environment variables) |
| Caching | In-memory + pre-aggregations stored as Parquet on S3 |
| Deployment | Docker container on ECS, or Cube Cloud (managed) |
| Auth | JWT-based, integrates with your identity provider |
| BI Integration | Native connectors for Preset, Metabase, Streamlit, Grafana |
| LLM Integration | REST API is perfect for AI agents to query structured metrics |
| Refresh | Configurable refresh keys per pre-aggregation |

---

## 8. Consumers — BI, Apps & AI Agents

**What:** Everything that consumes your data: BI dashboards, internal apps, data science notebooks, LLM-powered agents. All query through Cube's API or directly against Athena.

**Why this approach:**
- Cube API gives every consumer consistent metrics
- BI tools (Preset/Superset, Lightdash, Metabase) connect to Cube or Athena
- Data science notebooks (Jupyter) can query Athena via PyAthena
- LLM agents can call Cube's REST API for structured analytics
- Internal apps use Cube's GraphQL/REST for embedded analytics

**Python Notebook:**

```python
import pyathena
conn = pyathena.connect(
    s3_staging_dir="s3://my-datalake/athena-results/",
    region_name="eu-north-1"
)
df = pd.read_sql("SELECT * FROM marts.fct_revenue", conn)
```

**LLM Agent:**

```python
import requests
response = requests.get(
    "https://my-cube.example.com/cubejs-api/v1/load",
    params={"query": json.dumps({
        "measures": ["Revenue.total_revenue"],
        "timeDimensions": [{
            "dimension": "Revenue.order_date",
            "granularity": "month"
        }]
    })},
    headers={"Authorization": "Bearer <token>"}
)
metrics = response.json()
```

**Configuration:**

| Setting | Value |
|---|---|
| BI Tool | Preset (hosted Superset) or Lightdash for open-source-friendly option |
| Notebooks | Jupyter + PyAthena for data science |
| LLM Agents | Cube REST API → structured data → LLM generates insights |
| Embedded Analytics | Cube's REST/GraphQL for building dashboards in your app |
| Reverse ETL | Census or Hightouch to push data back to SaaS tools |
| Alerting | Set up alerts on metric thresholds via Cube or your BI tool |

---

## Monthly Cost Estimate

| Service | Cost | Notes |
|---|---|---|
| S3 Storage | ~$23/TB/month | Your entire data lake |
| Athena Queries | $5/TB scanned | Iceberg minimizes scans via metadata |
| Glue Catalog | ~$0 | First 1M objects free |
| dlt | Free | Open source Python library |
| dbt Core | Free | Open source (dbt Cloud is paid) |
| Dagster | Free / $$$ | Open source or Dagster Cloud |
| Cube.dev | Free / $$$ | Open source or Cube Cloud |

**Bottom line:** For a small-to-mid team processing <1TB/day, your AWS bill will likely be **$50–200/month**. The majority of the stack is open source and runs on minimal compute (a single ECS task for Dagster + Cube).

---

## Implementation Order

1. **S3 bucket + Glue Catalog** — set up the foundation
2. **dlt** — load one source into Iceberg
3. **dbt + Athena** — build staging and mart models
4. **Dagster** — add when you need scheduling
5. **Cube.dev** — add when you need a semantic layer
6. **BI / LLM agents** — connect consumers last

Each layer is independent. Add them one at a time. Ship value early.
