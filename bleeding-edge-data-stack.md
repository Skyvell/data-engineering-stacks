# Most Modern Data Engineering Stack — April 2026

> **BLEEDING EDGE** — No Iceberg. No Glue. No Athena. No dbt.

```
dlt → S3 (Parquet) + PostgreSQL (DuckLake) → SQLMesh + DuckDB/MotherDuck → S3 → Cube.dev → LLM Agents
```

Orchestrated by **Dagster**. Quality checked by **SQLMesh Audits + Soda**.

---

## 1. Ingestion — dlt (Data Load Tool)

**What:** Python-native ELT framework. Extracts from APIs, databases, files and loads directly to your DuckLake tables on S3. No infrastructure, no connectors to host — just `pip install dlt`.

**Why dlt:**
- No separate infrastructure — just a Python library
- Automatic schema inference and evolution on write
- Built-in DuckDB destination + S3 Parquet support
- Handles pagination, rate limiting, retries, incremental loading
- Version-controlled pipelines in Git
- Free and open source — no per-connector licensing
- Pairs with Sling for database-to-database CDC replication

**Code Example:**

```python
import dlt

@dlt.source
def my_api():
    @dlt.resource(
        write_disposition="merge",
        primary_key="id"
    )
    def orders():
        yield from paginated_api_call("/orders")
    
    @dlt.resource(
        write_disposition="merge",
        primary_key="id",
        merge_key="updated_at"  # SCD Type 1
    )
    def customers():
        yield from paginated_api_call("/customers")
    
    return orders, customers

# Load into DuckDB/MotherDuck → DuckLake on S3
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="motherduck",
    dataset_name="raw"
)
pipeline.run(my_api())
```

**For database sources, use Sling alongside dlt:**

```bash
sling run --src-conn POSTGRES --tgt-conn MOTHERDUCK \
  --src-stream "public.events" --mode incremental
```

**Configuration:**

| Setting | Value |
|---|---|
| Destination | `motherduck` or `duckdb` (writes to DuckLake) |
| Write Disposition | `append`, `replace`, or `merge` (SCD Type 1/2) |
| Incremental | Cursor-based and merge-based incremental loads |
| File Format | Parquet (optimal for columnar analytics) |
| Parallelism | Configurable worker count for concurrent extraction |
| Schema Contracts | Enforce or evolve schemas on write |
| Sources | 200+ verified: Salesforce, Stripe, PostgreSQL, MySQL, REST APIs, etc. |

---

## 2. Storage — S3 + DuckLake

**What:** Data lives as Parquet files on S3. DuckLake replaces both Iceberg AND the Glue Catalog by storing all metadata in a PostgreSQL database. The catalog IS the metadata — no separate metadata files on S3, no manifest files, no snapshot JSON. Radically simpler.

**Why DuckLake over Iceberg + Glue:**
- Eliminates Iceberg's metadata complexity (no manifest files, no snapshot JSONs on S3)
- Eliminates Glue Catalog — PostgreSQL IS the catalog
- 926× faster queries and 105× faster ingestion vs Iceberg for streaming workloads
- No small files problem — DuckLake inlines small writes directly in the catalog
- ACID transactions via PostgreSQL
- Schema evolution, time travel, partition evolution — all via SQL queries on the catalog
- Multi-table atomicity (commit across multiple tables in one transaction)
- Interop: can import/export to Iceberg if needed (DuckLake 0.3+)
- Open format — not locked to DuckDB

**Architecture Comparison:**

```
Traditional Iceberg:
  S3: data files + metadata.json + manifest-list.avro + manifest.avro
  Glue Catalog: pointer to metadata.json
  (multiple S3 reads just to find which files to query)

DuckLake:
  S3: data files only (Parquet)
  PostgreSQL: ALL metadata (schemas, tables, columns, 
              file locations, statistics, snapshots)
  (one SQL query to find which files to query = milliseconds)
```

**Setup:**

```sql
INSTALL ducklake;
LOAD ducklake;

ATTACH 'postgres:dbname=ducklake_catalog host=my-rds.amazonaws.com' 
  AS my_lake (TYPE ducklake, DATA_PATH 's3://my-datalake/warehouse/');

CREATE SCHEMA my_lake.raw;
CREATE SCHEMA my_lake.staging;
CREATE SCHEMA my_lake.marts;

-- Time travel:
SELECT * FROM my_lake.raw.orders 
  AT (TIMESTAMP => '2026-03-01');

-- Import existing Iceberg tables (metadata-only, no data copy):
CALL iceberg_to_ducklake('iceberg_catalog', 'my_lake');
```

**S3 Structure (clean — no metadata clutter):**

```
s3://my-datalake/
  └── warehouse/
      ├── raw/orders/       → Parquet files only
      ├── raw/customers/    → Parquet files only
      ├── staging/stg_*     → Parquet files only
      └── marts/fct_*       → Parquet files only
```

**Configuration:**

| Setting | Value |
|---|---|
| Catalog Backend | PostgreSQL on RDS (~$15/month for db.t4g.micro) |
| Data Storage | S3 Standard ($0.023/GB/month) |
| Data Format | Parquet with Snappy/Zstd compression |
| Data Inlining | Small writes stored in PostgreSQL, flushed to S3 on checkpoint |
| Iceberg Interop | DuckLake 0.3+ supports bidirectional copy with Iceberg |
| Encryption | Plans for row/column-level encryption via catalog-held keys |
| Cost | ~$23/TB/month (S3) + ~$15/month (RDS) |

---

## 3. Compute — DuckDB / MotherDuck

**What:** DuckDB is the query engine — replaces Athena, Spark, Trino. Runs locally for development, scales to MotherDuck (cloud DuckDB) for production. Each user gets their own "Duckling" instance. Dual execution: heavy queries run in the cloud, light queries run locally.

**Why DuckDB/MotherDuck:**
- No clusters to manage — single process, in-memory columnar engine
- 100x faster than Spark on local Parquet benchmarks
- Dual execution — queries split between local and cloud automatically
- Per-user Ducklings — no resource contention between analysts
- Reads S3 Parquet/DuckLake natively
- DuckDB-WASM — can even run in the browser for embedded analytics
- Handles multi-TB scans with disk spill on a single node
- MotherDuck scales via vertical (bigger Ducklings) and horizontal (more Ducklings)

**Code Example:**

```python
# Local development — pure DuckDB:
import duckdb

conn = duckdb.connect()
conn.install_extension("ducklake")
conn.load_extension("ducklake")

conn.sql("""
    ATTACH 'postgres:dbname=ducklake_catalog host=localhost' 
    AS lake (TYPE ducklake, DATA_PATH 's3://my-datalake/warehouse/')
""")

df = conn.sql("""
    SELECT customer_segment, SUM(revenue) as total
    FROM lake.marts.fct_revenue
    WHERE order_date >= '2026-01-01'
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# Production — MotherDuck:
conn = duckdb.connect("md:my_database?motherduck_token=<token>")
```

**Configuration:**

| Setting | Value |
|---|---|
| Local Dev | DuckDB CLI or Python — zero setup, instant start |
| Production | MotherDuck Business ($250/month) or self-hosted DuckDB on ECS |
| Duckling Sizes | pulse, standard, jumbo, mega, giga (vertical scaling) |
| Concurrency | Per-user Ducklings + read replicas for high concurrency |
| Memory | Spills to disk when RAM exhausted |
| Sweet Spot | < 10TB working set, < 50 concurrent heavy queries |
| Escape Hatch | DuckLake → Iceberg export → Spark/Trino if you outgrow it |

---

## 4. Transformation — SQLMesh

**What:** SQLMesh replaces dbt with a smarter transformation framework. Column-level lineage, virtual environments (test transforms without copying data), automatic change categorization, and built-in scheduler. What dbt would be if rebuilt from scratch today.

**Why SQLMesh over dbt:**
- Column-level lineage — knows exactly which columns are affected by changes
- Virtual environments — test model changes without duplicating data or compute
- Automatic change categorization — distinguishes breaking vs non-breaking changes
- Smart incremental — only recomputes what actually changed
- Built-in CI/CD — plan/apply workflow like Terraform for data
- 9× faster execution and cost savings vs dbt (Databricks benchmark)
- Backwards-compatible with dbt projects — can migrate incrementally
- Native DuckDB support
- Contributed to Linux Foundation (March 2026) — neutral governance

**Project Structure:**

```
my_sqlmesh_project/
  ├── config.yaml
  ├── models/
  │   ├── staging/
  │   │   ├── stg_orders.sql
  │   │   └── stg_customers.sql
  │   ├── intermediate/
  │   │   └── int_orders_enriched.sql
  │   └── marts/
  │       ├── fct_revenue.sql
  │       └── dim_customers.sql
  ├── audits/
  │   └── assert_revenue_positive.sql
  ├── macros/
  └── seeds/
```

**Config:**

```yaml
# config.yaml
gateways:
  local:
    connection:
      type: duckdb
      database: md:my_database   # MotherDuck
      extensions:
        - ducklake

model_defaults:
  dialect: duckdb
```

**Model Example:**

```sql
-- models/marts/fct_revenue.sql
MODEL (
  name marts.fct_revenue,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_date,
    batch_size 7,
  ),
  grain (order_id),
  audits (
    assert_positive_values(columns=[revenue]),
    not_null(columns=[order_id, customer_id]),
  ),
);

SELECT
    o.order_id,
    o.order_date,
    c.customer_segment,
    SUM(o.amount) AS revenue
FROM staging.stg_orders AS o
JOIN staging.stg_customers AS c 
  ON o.customer_id = c.customer_id
WHERE o.order_date BETWEEN @start_date AND @end_date
GROUP BY 1, 2, 3;
```

**CLI Workflow (Terraform-like):**

```bash
sqlmesh plan          # shows what will change (column-level diff)
sqlmesh apply         # executes changes
sqlmesh plan dev      # test in virtual environment (no data copy!)
sqlmesh diff prod dev # compare environments
```

**Configuration:**

| Setting | Value |
|---|---|
| Engine | DuckDB / MotherDuck (native support) |
| Migration from dbt | `sqlmesh init --dbt` converts existing projects |
| Virtual Environments | Test changes without duplicating data or compute |
| Column-Level Lineage | Auto-detected, no config needed |
| Change Categories | Breaking (reprocess downstream) vs Non-breaking (forward-only) |
| Scheduler | Built-in cron, or integrate with Dagster/Airflow |
| CI/CD | `sqlmesh plan --auto-apply` in CI pipeline |
| Audit Framework | Built-in data quality checks |

---

## 5. Orchestration — Dagster

**What:** Schedules and monitors the entire pipeline. Asset-based: each dlt source and SQLMesh model is a data asset with dependencies, freshness expectations, and observability. Optional for simple setups — SQLMesh has a built-in scheduler.

**Why Dagster:**
- Asset-based — maps naturally to data sources and models
- First-class dlt integration (`dagster-dlt`)
- SQLMesh integration (`dagster-sqlmesh`)
- Freshness policies — define SLAs per asset
- Auto-materialization — trigger downstream when upstream refreshes
- Sensors for event-driven runs (S3 file arrival, webhooks)
- Excellent local dev experience (`dagster dev`)

**When to use Dagster vs SQLMesh's built-in scheduler:**

| Scenario | Use |
|---|---|
| Single pipeline, small team | SQLMesh scheduler alone |
| Multiple dlt sources, different schedules | Add Dagster |
| Cross-system orchestration (dlt + SQLMesh + Cube refresh) | Add Dagster |
| Asset-level freshness monitoring | Add Dagster |
| Event-driven triggers (S3 events, webhooks) | Add Dagster |

**Code Example:**

```python
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster import Definitions, ScheduleDefinition, define_asset_job

@dlt_assets(
    dlt_source=my_api(),
    dlt_pipeline=pipeline(
        pipeline_name="my_pipeline",
        destination="motherduck",
        dataset_name="raw"
    ),
    name="raw_data",
    group_name="ingestion",
)
def raw_assets(context, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

from dagster_sqlmesh import sqlmesh_assets

@sqlmesh_assets(config=sqlmesh_config)
def transform_assets(context):
    ...

daily_pipeline = ScheduleDefinition(
    job=define_asset_job("daily_pipeline", selection="*"),
    cron_schedule="0 6 * * *",
)
```

**Configuration:**

| Setting | Value |
|---|---|
| Deployment | Dagster Cloud (managed) or ECS/K8s (self-hosted) |
| dagster-dlt | `pip install dagster-dlt` |
| dagster-sqlmesh | `pip install dagster-sqlmesh` |
| Freshness Policies | e.g. orders must be < 6 hours old |
| Auto-Materialization | Trigger downstream on upstream refresh |
| Optional | SQLMesh's built-in scheduler handles simple cases alone |

---

## 6. Data Quality — SQLMesh Audits + Soda

**What:** Two-layer approach: SQLMesh audits for model-level tests (not-null, unique, custom assertions), and Soda for data observability (anomaly detection, freshness monitoring, schema drift alerts).

**SQLMesh Audits (built into models):**

```sql
MODEL (
  name marts.fct_revenue,
  audits (
    not_null(columns=[order_id, revenue, order_date]),
    unique_values(columns=[order_id]),
    assert_positive_values(columns=[revenue]),
    accepted_range(column=revenue, min_value=0, max_value=1000000),
  ),
);
```

**Custom Audit:**

```sql
-- audits/assert_revenue_matches_orders.sql
AUDIT (
  name assert_revenue_matches_orders,
  dialect duckdb,
);
SELECT order_id
FROM @this_model
WHERE revenue != quantity * unit_price;
```

**Soda Checks:**

```yaml
# checks.yml
checks for raw.orders:
  - freshness(updated_at) < 6h
  - row_count > 0
  - anomaly detection for row_count
  - schema:
      fail:
        when forbidden column present: [ssn, credit_card]

checks for marts.fct_revenue:
  - anomaly detection for avg(revenue)
  - duplicate_count(order_id) = 0
  - failed rows:
      fail condition: revenue < 0
```

**Configuration:**

| Setting | Value |
|---|---|
| SQLMesh Audits | Run automatically on plan/apply |
| Soda Core | Free, open source (`pip install soda-core-duckdb`) |
| Soda Cloud | Paid — dashboards, ML anomaly detection, Slack alerts |
| Integration | Dagster sensor triggers Soda scan after each pipeline run |

---

## 7. Semantic Layer — Cube.dev

**What:** Defines business metrics once in code, exposes them via REST/GraphQL API. Any consumer — BI tools, notebooks, LLM agents — gets consistent, cached metrics. Pre-aggregations mean most queries never hit DuckDB at all.

**Why Cube.dev:**
- Define metrics once, consume everywhere
- Built-in caching + pre-aggregation = sub-second responses
- REST and GraphQL APIs for any consumer
- Perfect for LLM agents — structured API beats raw SQL
- Access control per role/user
- Supports DuckDB/MotherDuck as data source

**Schema Example:**

```javascript
// cube/schema/Revenue.js
cube('Revenue', {
  sql_table: 'lake.marts.fct_revenue',

  measures: {
    total_revenue: {
      type: 'sum',
      sql: 'revenue',
      format: 'currency',
    },
    order_count: { type: 'count' },
    avg_order_value: {
      type: 'number',
      sql: `${total_revenue} / NULLIF(${order_count}, 0)`,
      format: 'currency',
    },
  },

  dimensions: {
    order_date: { type: 'time', sql: 'order_date' },
    customer_segment: { type: 'string', sql: 'customer_segment' },
  },

  pre_aggregations: {
    daily_by_segment: {
      measures: [CUBE.total_revenue, CUBE.order_count],
      dimensions: [CUBE.customer_segment],
      time_dimension: CUBE.order_date,
      granularity: 'day',
      refresh_key: { every: '1 hour' },
    },
  },
});
```

**Configuration:**

| Setting | Value |
|---|---|
| Data Source | DuckDB / MotherDuck connection |
| Caching | In-memory + pre-aggregations on S3 |
| Deployment | Docker on ECS or Cube Cloud (managed) |
| Auth | JWT-based with role-based access control |
| BI Integration | Preset, Metabase, Streamlit, Grafana |
| LLM Integration | REST API for AI agents to query structured metrics |
| MCP | MotherDuck supports MCP — connect Claude/Cursor directly |

---

## 8. Consumers — LLM Agents + BI

**What:** The bleeding-edge consumption layer is LLM agents as the primary interface. Users ask questions in natural language, the agent queries Cube's API, and returns insights. Traditional BI becomes secondary.

**Consumer Options:**

| Consumer | How |
|---|---|
| LLM Agents (primary) | Cube REST API — structured, cached, governed |
| Ad-hoc Exploration | MotherDuck MCP for Claude/Cursor direct access |
| Embedded Analytics | MotherDuck Dives — iframe into your app |
| BI Dashboards | Evidence.dev (code-first) or Preset (hosted Superset) |
| Data Science | Jupyter + DuckDB + Polars (not Pandas) |
| Reverse ETL | Census or Hightouch for pushing to SaaS tools |
| Alerting | Soda + Cube threshold alerts via Slack/PagerDuty |

**LLM Agent via Cube API:**

```python
import requests, json

def ask_data(question: str) -> dict:
    cube_query = llm_to_cube_query(question)
    response = requests.get(
        "https://cube.example.com/cubejs-api/v1/load",
        params={"query": json.dumps(cube_query)},
        headers={"Authorization": f"Bearer {token}"}
    )
    return response.json()
```

**Data Science with Polars (not Pandas):**

```python
import duckdb
conn = duckdb.connect("md:my_database")
lf = conn.sql("SELECT * FROM lake.marts.fct_revenue").pl()
```

---

## Bleeding Edge vs Production-Safe Comparison

| Component | Safe Choice | Bleeding Edge | Why Switch |
|---|---|---|---|
| Table Format | Apache Iceberg | DuckLake | SQL-based catalog, 926× faster metadata |
| Catalog | AWS Glue Catalog | PostgreSQL (built into DuckLake) | No separate service, no metadata files |
| Compute | Athena ($5/TB scanned) | DuckDB / MotherDuck | 100× faster than Spark, dual execution |
| Transforms | dbt | SQLMesh | Column lineage, virtual envs, 9× faster |
| Quality | dbt tests + Great Expectations | SQLMesh audits + Soda | ML anomaly detection, built-in audits |
| BI | Preset / Looker dashboards | LLM agents + Evidence.dev | Natural language analytics, code-first |
| Data Science | Pandas + PyAthena | Polars + DuckDB (zero-copy) | 10-100× faster than Pandas |

---

## Monthly Cost Estimate

| Service | Cost | Notes |
|---|---|---|
| S3 Storage | ~$23/TB/month | Parquet data files only (no metadata files!) |
| RDS PostgreSQL | ~$15/month | DuckLake catalog (db.t4g.micro) |
| MotherDuck | $0–250/month | Free tier or Business plan |
| dlt | Free | Open source |
| SQLMesh | Free | Open source (Tobiko Cloud is paid) |
| Dagster | Free / $$$ | Open source or Dagster Cloud |
| Cube.dev | Free / $$$ | Open source or Cube Cloud |
| Soda Core | Free | Open source (Soda Cloud is paid) |

**Bottom line:** For a small team with <5TB, this stack runs on **$40–300/month** total AWS spend. If you use MotherDuck Free tier + self-host Cube/Dagster, the floor is ~$40/month (S3 + RDS).

---

## Risks & Mitigations

| Risk | Detail | Mitigation |
|---|---|---|
| DuckLake ecosystem | Fewer engine integrations than Iceberg. Spark/Trino connectors in progress. | DuckLake 0.3 exports to Iceberg — you can migrate if needed. |
| MotherDuck pricing | Lite plan removed, Business jumped to $250/month. Changed 3 times in 18 months. | Self-host DuckDB on ECS as fallback. Data is on S3 — not locked in. |
| SQLMesh maturity | Smaller community than dbt. Fewer tutorials and Stack Overflow answers. | SQLMesh imports dbt projects. Can migrate back if needed. Now Linux Foundation governed. |
| Single-node limits | DuckDB won't handle 50TB+ scans or 100+ concurrent heavy users. | Export DuckLake → Iceberg → Spark/Trino when you hit the ceiling. |
| LLM reliability | AI agents can hallucinate metrics or misinterpret questions. | Cube semantic layer constrains the LLM to defined metrics — can't make up numbers. |

---

## Implementation Order

1. **S3 bucket + RDS PostgreSQL** — set up DuckLake foundation
2. **dlt** — load one source into DuckLake
3. **SQLMesh + DuckDB** — build staging and mart models
4. **Soda** — add data quality checks
5. **Dagster** — add when you need scheduling beyond SQLMesh's built-in
6. **Cube.dev** — add when you need a semantic layer
7. **LLM agents / BI** — connect consumers last

Each layer is independent. Add them one at a time. Ship value early.
