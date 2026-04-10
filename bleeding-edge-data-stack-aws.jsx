import { useState } from "react";

const layers = [
  {
    id: "ingestion",
    title: "1. Ingestion — dlt",
    subtitle: "Data Load Tool",
    color: "#2563eb",
    icon: "↓",
    details: {
      what: "Python-native ELT framework. Extracts from APIs, databases, files and loads directly to your DuckLake tables on S3. No infrastructure, no connectors to host — just pip install dlt.",
      why: [
        "No separate infrastructure — just a Python library",
        "Automatic schema inference and evolution on write",
        "Built-in DuckDB destination + S3 Parquet support",
        "Handles pagination, rate limiting, retries, incremental loading",
        "Version-controlled pipelines in Git",
        "Free and open source — no per-connector licensing",
        "Pairs with Sling for database-to-database CDC replication",
      ],
      how: `import dlt

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

# For database sources, use Sling alongside dlt:
# sling run --src-conn POSTGRES --tgt-conn MOTHERDUCK \\
#   --src-stream "public.events" --mode incremental`,
      config: {
        "Destination": "motherduck or duckdb (writes to DuckLake)",
        "Write Disposition": "append, replace, or merge (SCD Type 1/2)",
        "Incremental": "Cursor-based and merge-based incremental loads",
        "File Format": "Parquet (optimal for columnar analytics)",
        "Parallelism": "Configurable worker count for concurrent extraction",
        "Sling": "Add for database CDC (Postgres, MySQL → MotherDuck)",
        "Schema Contracts": "Enforce or evolve schemas on write",
      },
    },
  },
  {
    id: "storage",
    title: "2. Storage — S3 + DuckLake",
    subtitle: "Next-Gen Lakehouse Format",
    color: "#059669",
    icon: "◆",
    details: {
      what: "Data lives as Parquet files on S3. DuckLake replaces both Iceberg AND the Glue Catalog by storing all metadata in a PostgreSQL database. The catalog IS the metadata — no separate metadata files on S3, no manifest files, no snapshot JSON. Radically simpler.",
      why: [
        "Eliminates Iceberg's metadata complexity (no manifest files, no snapshot JSONs on S3)",
        "Eliminates Glue Catalog — PostgreSQL IS the catalog",
        "926× faster queries and 105× faster ingestion vs Iceberg for streaming workloads",
        "No small files problem — DuckLake inlines small writes directly in the catalog",
        "ACID transactions via PostgreSQL",
        "Schema evolution, time travel, partition evolution — all via SQL queries on the catalog",
        "Multi-table atomicity (commit across multiple tables in one transaction)",
        "Interop: can import/export to Iceberg if needed (DuckLake 0.3+)",
        "Open format — not locked to DuckDB",
      ],
      how: `-- DuckLake Architecture:
--
-- Traditional Iceberg:
--   S3: data files + metadata.json + manifest-list.avro + manifest.avro
--   Glue Catalog: pointer to metadata.json
--   (multiple S3 reads just to find which files to query)
--
-- DuckLake:
--   S3: data files only (Parquet)
--   PostgreSQL: ALL metadata (schemas, tables, columns, 
--               file locations, statistics, snapshots)
--   (one SQL query to find which files to query = milliseconds)

-- Setup DuckLake with PostgreSQL catalog:
INSTALL ducklake;
LOAD ducklake;

ATTACH 'postgres:dbname=ducklake_catalog host=my-rds.amazonaws.com' 
  AS my_lake (TYPE ducklake, DATA_PATH 's3://my-datalake/warehouse/');

-- Create tables — metadata goes to Postgres, data goes to S3:
CREATE SCHEMA my_lake.raw;
CREATE SCHEMA my_lake.staging;
CREATE SCHEMA my_lake.marts;

-- S3 Structure (clean — no metadata clutter):
-- s3://my-datalake/
--   └── warehouse/
--       ├── raw/orders/       → Parquet files only
--       ├── raw/customers/    → Parquet files only
--       ├── staging/stg_*     → Parquet files only
--       └── marts/fct_*       → Parquet files only

-- Time travel (powered by PostgreSQL snapshots):
SELECT * FROM my_lake.raw.orders 
  AT (TIMESTAMP => '2026-03-01');

-- Import existing Iceberg tables (metadata-only, no data copy):
CALL iceberg_to_ducklake('iceberg_catalog', 'my_lake');`,
      config: {
        "Catalog Backend": "PostgreSQL on RDS (~$15/month for db.t4g.micro)",
        "Data Storage": "S3 Standard ($0.023/GB/month)",
        "Data Format": "Parquet with Snappy/Zstd compression",
        "Data Inlining": "Small writes stored in PostgreSQL, flushed to S3 on checkpoint",
        "Checkpoint": "Configurable — flush inlined data to Parquet periodically",
        "Iceberg Interop": "DuckLake 0.3+ supports bidirectional copy with Iceberg",
        "Encryption": "Plans for row/column-level encryption via catalog-held keys",
        "Cost": "~$23/TB/month (S3) + ~$15/month (RDS PostgreSQL) = fraction of Glue+Iceberg",
      },
    },
  },
  {
    id: "compute",
    title: "3. Compute — DuckDB / MotherDuck",
    subtitle: "Embedded + Cloud Analytics Engine",
    color: "#dc2626",
    icon: "⚡",
    details: {
      what: "DuckDB is the query engine — replaces Athena, Spark, Trino. Runs locally for development, scales to MotherDuck (cloud DuckDB) for production. Each user gets their own 'Duckling' instance. Dual execution: heavy queries run in the cloud, light queries run locally.",
      why: [
        "No clusters to manage — single process, in-memory columnar engine",
        "100x faster than Spark on local Parquet benchmarks",
        "Dual execution — queries split between local and cloud automatically",
        "Per-user Ducklings — no resource contention between analysts",
        "Reads S3 Parquet/DuckLake natively",
        "Supports dbt via dbt-duckdb adapter",
        "DuckDB-WASM — can even run in the browser for embedded analytics",
        "Handles multi-TB scans with disk spill on a single node",
        "MotherDuck scales via vertical (bigger Ducklings) and horizontal (more Ducklings)",
      ],
      how: `# Local development — pure DuckDB:
import duckdb

conn = duckdb.connect()
conn.install_extension("ducklake")
conn.load_extension("ducklake")

# Attach to your DuckLake:
conn.sql("""
    ATTACH 'postgres:dbname=ducklake_catalog host=localhost' 
    AS lake (TYPE ducklake, DATA_PATH 's3://my-datalake/warehouse/')
""")

# Query your data lake like a local database:
df = conn.sql("""
    SELECT customer_segment, SUM(revenue) as total
    FROM lake.marts.fct_revenue
    WHERE order_date >= '2026-01-01'
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# Production — MotherDuck (cloud DuckDB):
conn = duckdb.connect("md:my_database?motherduck_token=<token>")

# Dual execution — this query runs partly local, partly cloud:
conn.sql("""
    SELECT local_table.id, cloud_table.revenue
    FROM local_table
    JOIN lake.marts.fct_revenue AS cloud_table
    ON local_table.id = cloud_table.customer_id
""")

# dbt profiles.yml for MotherDuck:
# my_project:
#   target: prod
#   outputs:
#     prod:
#       type: duckdb
#       path: "md:my_database"
#       extensions:
#         - ducklake`,
      config: {
        "Local Dev": "DuckDB CLI or Python — zero setup, instant start",
        "Production": "MotherDuck Business ($250/month) or self-hosted DuckDB on ECS",
        "Duckling Sizes": "pulse, standard, jumbo, mega, giga (vertical scaling)",
        "Concurrency": "Per-user Ducklings + read replicas for high concurrency",
        "dbt Adapter": "dbt-duckdb (pip install dbt-duckdb)",
        "Memory": "Spills to disk when RAM exhausted — handles larger-than-memory queries",
        "Sweet Spot": "< 10TB working set, < 50 concurrent heavy queries",
        "Escape Hatch": "If you outgrow it, DuckLake → Iceberg export → Spark/Trino",
      },
    },
  },
  {
    id: "transform",
    title: "4. Transformation — SQLMesh",
    subtitle: "Next-Gen dbt Alternative",
    color: "#ea580c",
    icon: "⟳",
    details: {
      what: "SQLMesh replaces dbt with a smarter transformation framework. Column-level lineage, virtual environments (test transforms without copying data), automatic change categorization, and built-in scheduler. It's what dbt would be if rebuilt from scratch today.",
      why: [
        "Column-level lineage — knows exactly which columns are affected by changes",
        "Virtual environments — test model changes without duplicating data or compute",
        "Automatic change categorization — distinguishes breaking vs non-breaking changes",
        "Smart incremental — only recomputes what actually changed (not just new rows)",
        "Built-in CI/CD — plan/apply workflow like Terraform for data",
        "Built-in scheduler (no Dagster needed for simple setups)",
        "Backwards-compatible with dbt projects — can migrate incrementally",
        "Native DuckDB support",
      ],
      how: `# Project structure:
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
  ├── audits/                    # (tests in dbt terms)
  │   └── assert_revenue_positive.sql
  ├── macros/
  └── seeds/

# config.yaml:
gateways:
  local:
    connection:
      type: duckdb
      database: md:my_database   # MotherDuck
      extensions:
        - ducklake

model_defaults:
  dialect: duckdb

# models/marts/fct_revenue.sql:
MODEL (
  name marts.fct_revenue,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_date,
    batch_size 7,           # process 7 days at a time
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

# CLI workflow (Terraform-like):
# sqlmesh plan          → shows what will change (column-level diff)
# sqlmesh apply         → executes changes
# sqlmesh plan dev      → test in virtual environment (no data copy!)
# sqlmesh diff prod dev → compare environments`,
      config: {
        "Engine": "DuckDB / MotherDuck (native support)",
        "Migration from dbt": "sqlmesh init --dbt converts existing dbt projects",
        "Virtual Environments": "Test changes without duplicating data or compute",
        "Column-Level Lineage": "Auto-detected, no config needed",
        "Change Categories": "Breaking (reprocess downstream) vs Non-breaking (forward-only)",
        "Scheduler": "Built-in cron scheduler, or integrate with Dagster/Airflow",
        "CI/CD": "sqlmesh plan --auto-apply in CI pipeline",
        "Audit Framework": "Built-in data quality checks (replaces dbt tests + Great Expectations)",
      },
    },
  },
  {
    id: "orchestration",
    title: "5. Orchestration — Dagster",
    subtitle: "Asset-Based Orchestration",
    color: "#0891b2",
    icon: "◎",
    details: {
      what: "Schedules and monitors the entire pipeline. Asset-based: each dlt source and SQLMesh model is a data asset with dependencies, freshness expectations, and observability. Note: SQLMesh has a built-in scheduler — Dagster is optional for simple setups but essential for complex multi-system orchestration.",
      why: [
        "Asset-based — maps naturally to data sources and models",
        "First-class dlt integration (dagster-dlt)",
        "SQLMesh integration via dagster-sqlmesh",
        "Freshness policies — define SLAs per asset",
        "Auto-materialization — trigger downstream when upstream refreshes",
        "Sensors for event-driven runs (S3 file arrival, webhooks)",
        "Excellent local dev experience (dagster dev)",
        "Dagster Cloud for managed deployment",
      ],
      how: `# When to use Dagster vs SQLMesh's built-in scheduler:
#
# SQLMesh scheduler alone:
#   - dlt loads on cron → SQLMesh transforms on cron
#   - Simple, fewer moving parts
#   - Good for: single pipeline, small team
#
# Add Dagster when:
#   - Multiple dlt sources with different schedules
#   - Need cross-system orchestration (dlt + SQLMesh + Cube refresh)
#   - Need asset-level freshness monitoring
#   - Need sensors (trigger on S3 events, webhooks)
#   - Multiple teams need visibility

from dagster_dlt import DagsterDltResource, dlt_assets
from dagster import (
    Definitions, ScheduleDefinition, 
    define_asset_job, AssetSelection,
    FreshnessPolicy
)

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

# SQLMesh assets — auto-imported from your SQLMesh project
from dagster_sqlmesh import SQLMeshDagsterTranslator, sqlmesh_assets

@sqlmesh_assets(
    config=sqlmesh_config,
    dagster_sqlmesh_translator=SQLMeshDagsterTranslator(),
)
def transform_assets(context):
    ...

# Schedules
daily_pipeline = ScheduleDefinition(
    job=define_asset_job(
        "daily_pipeline",
        selection=AssetSelection.all()
    ),
    cron_schedule="0 6 * * *",
)`,
      config: {
        "Deployment": "Dagster Cloud (managed) or ECS/K8s (self-hosted)",
        "dagster-dlt": "pip install dagster-dlt",
        "dagster-sqlmesh": "pip install dagster-sqlmesh",
        "Freshness Policies": "e.g. orders must be < 6 hours old",
        "Auto-Materialization": "Trigger downstream on upstream refresh",
        "Sensors": "S3 file arrival, webhook, schedule-based",
        "Optional": "SQLMesh's built-in scheduler handles simple cases alone",
      },
    },
  },
  {
    id: "quality",
    title: "6. Data Quality — Soda + SQLMesh Audits",
    subtitle: "Observability & Testing",
    color: "#f59e0b",
    icon: "✓",
    details: {
      what: "Two-layer quality approach: SQLMesh audits for model-level tests (not-null, unique, custom assertions), and Soda for data observability (anomaly detection, freshness monitoring, schema drift alerts). Together they catch both logic bugs and data drift.",
      why: [
        "SQLMesh audits run on every plan/apply — catches issues before deployment",
        "Soda scans run on schedule — catches upstream data quality drift",
        "Soda anomaly detection — ML-based, no thresholds to manually set",
        "Freshness monitoring — alerts when sources stop updating",
        "Schema drift detection — know when upstream schemas change",
        "Soda Cloud for dashboards, incidents, and alerting",
        "Both support DuckDB natively",
      ],
      how: `# SQLMesh Audits (built into model definitions):
# models/marts/fct_revenue.sql
MODEL (
  name marts.fct_revenue,
  audits (
    not_null(columns=[order_id, revenue, order_date]),
    unique_values(columns=[order_id]),
    assert_positive_values(columns=[revenue]),
    accepted_range(column=revenue, min_value=0, max_value=1000000),
  ),
);

# Custom audit:
# audits/assert_revenue_matches_orders.sql
AUDIT (
  name assert_revenue_matches_orders,
  dialect duckdb,
);
SELECT order_id
FROM @this_model
WHERE revenue != quantity * unit_price;

# Soda checks (checks.yml):
checks for raw.orders:
  - freshness(updated_at) < 6h
  - row_count > 0
  - anomaly detection for row_count
  - schema:
      fail:
        when forbidden column present: [ssn, credit_card]
        when wrong type:
          order_id: integer
          amount: decimal

checks for marts.fct_revenue:
  - anomaly detection for avg(revenue)
  - duplicate_count(order_id) = 0
  - failed rows:
      fail condition: revenue < 0

# Run: soda scan -d motherduck -c soda_config.yml checks.yml`,
      config: {
        "SQLMesh Audits": "Run automatically on plan/apply — zero extra config",
        "Soda Core": "Free, open source (pip install soda-core-duckdb)",
        "Soda Cloud": "Paid — adds dashboards, anomaly detection, incidents, Slack alerts",
        "DuckDB Connection": "Native Soda DuckDB connector",
        "Freshness": "Monitor source table update timestamps",
        "Anomaly Detection": "ML-based on Soda Cloud, manual thresholds on Soda Core",
        "Integration": "Dagster sensor triggers Soda scan after each pipeline run",
      },
    },
  },
  {
    id: "semantic",
    title: "7. Semantic Layer — Cube.dev",
    subtitle: "Metrics API + Caching",
    color: "#c026d3",
    icon: "▣",
    details: {
      what: "Defines business metrics once in code, exposes them via REST/GraphQL API. Any consumer — BI tools, notebooks, LLM agents, internal apps — gets consistent, cached metrics. Single source of truth. Pre-aggregations mean most queries never hit DuckDB at all.",
      why: [
        "Define metrics once, consume everywhere",
        "Built-in caching + pre-aggregation = sub-second responses",
        "REST and GraphQL APIs for any consumer",
        "Perfect for LLM agents — structured API beats raw SQL",
        "Access control per role/user",
        "Supports DuckDB/MotherDuck as data source",
        "Open source core",
      ],
      how: `// cube/schema/Revenue.js
cube('Revenue', {
  sql_table: 'lake.marts.fct_revenue',  // DuckLake table

  measures: {
    total_revenue: {
      type: 'sum',
      sql: 'revenue',
      format: 'currency',
    },
    order_count: { type: 'count' },
    avg_order_value: {
      type: 'number',
      sql: \`\${total_revenue} / NULLIF(\${order_count}, 0)\`,
      format: 'currency',
    },
    revenue_growth: {
      type: 'number',
      sql: \`(\${total_revenue} - LAG(\${total_revenue}) OVER (ORDER BY \${order_date})) 
            / NULLIF(LAG(\${total_revenue}) OVER (ORDER BY \${order_date}), 0)\`,
      format: 'percent',
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
      // Pre-agg stored as Parquet on S3 — most queries 
      // served from here, never touching DuckDB
    },
  },
});

// LLM agent queries the API:
// "What was enterprise revenue last quarter?"
// → GET /cubejs-api/v1/load?query={
//     "measures": ["Revenue.total_revenue"],
//     "timeDimensions": [{"dimension": "Revenue.order_date", 
//       "dateRange": "last quarter"}],
//     "filters": [{"member": "Revenue.customer_segment",
//       "operator": "equals", "values": ["Enterprise"]}]
//   }
// → {"data": [{"Revenue.total_revenue": 2847291}]}`,
      config: {
        "Data Source": "DuckDB / MotherDuck connection",
        "Caching": "In-memory + pre-aggregations on S3",
        "Deployment": "Docker on ECS or Cube Cloud (managed)",
        "Auth": "JWT-based with role-based access control",
        "BI Integration": "Preset, Metabase, Streamlit, Grafana connectors",
        "LLM Integration": "REST API → AI agents query structured metrics",
        "MCP": "MotherDuck supports MCP — connect Claude/Cursor directly to your data",
      },
    },
  },
  {
    id: "consumers",
    title: "8. Consumers — LLM Agents + BI",
    subtitle: "AI-First Analytics",
    color: "#475569",
    icon: "◈",
    details: {
      what: "The bleeding-edge consumption layer is LLM agents as the primary interface. Users ask questions in natural language, the agent queries Cube's API (or DuckDB directly via MCP), and returns insights. Traditional BI dashboards still exist but become secondary to conversational analytics.",
      why: [
        "LLM agents via Cube REST API — structured, cached, access-controlled",
        "MotherDuck MCP — connect Claude/Cursor directly to your data warehouse",
        "MotherDuck Dives — embeddable interactive dashboards in your app",
        "Natural language → SQL (MotherDuck has built-in NL-to-SQL)",
        "Traditional BI (Preset, Evidence, Lightdash) for dashboards that still need to exist",
        "Jupyter + DuckDB for data science (zero-copy, instant)",
        "Reverse ETL via Census/Hightouch to push data back to SaaS tools",
      ],
      how: `# 1. LLM Agent via Cube API (recommended for production):
import requests, json

def ask_data(question: str) -> dict:
    """LLM agent tool — queries business metrics via Cube"""
    # LLM converts natural language to Cube query
    cube_query = llm_to_cube_query(question)
    response = requests.get(
        "https://cube.example.com/cubejs-api/v1/load",
        params={"query": json.dumps(cube_query)},
        headers={"Authorization": f"Bearer {token}"}
    )
    return response.json()

# 2. Direct MCP connection (for ad-hoc exploration):
# In Claude or Cursor settings, add MotherDuck MCP server
# → Claude can now query your data warehouse directly
# → "What were our top 10 customers last month?"
# → Claude writes SQL, executes against MotherDuck, returns results

# 3. MotherDuck Dives (embedded analytics):
# Create a Dive in MotherDuck UI
# Generate embed session token
# Drop iframe into your app:
# <iframe src="https://app.motherduck.com/dive/embed/..." />

# 4. Evidence.dev (code-first BI — bleeding edge):
# Write reports as Markdown + SQL
# Deploy as static site
# Evidence connects to MotherDuck directly

# 5. Jupyter for data science:
import duckdb
conn = duckdb.connect("md:my_database")
df = conn.sql("SELECT * FROM lake.marts.fct_revenue").df()

# Zero-copy to Polars (faster than Pandas):
import polars as pl
lf = conn.sql("SELECT * FROM lake.marts.fct_revenue").pl()`,
      config: {
        "Primary": "LLM agents via Cube REST API (structured, cached, governed)",
        "Ad-hoc": "MotherDuck MCP for Claude/Cursor direct access",
        "Embedded": "MotherDuck Dives for customer-facing analytics",
        "BI": "Evidence.dev (code-first) or Preset (hosted Superset)",
        "Data Science": "Jupyter + DuckDB + Polars (not Pandas)",
        "Reverse ETL": "Census or Hightouch for pushing to SaaS",
        "Alerting": "Soda + Cube threshold alerts via Slack/PagerDuty",
      },
    },
  },
];

const comparison = [
  { component: "Table Format", safe: "Apache Iceberg", bleeding: "DuckLake", why: "SQL-based catalog, 926× faster metadata" },
  { component: "Catalog", safe: "AWS Glue Catalog", bleeding: "PostgreSQL (built into DuckLake)", why: "No separate service, no metadata files" },
  { component: "Compute", safe: "Athena ($5/TB scanned)", bleeding: "DuckDB / MotherDuck", why: "100× faster than Spark, dual execution" },
  { component: "Transforms", safe: "dbt", bleeding: "SQLMesh", why: "Column lineage, virtual envs, smarter incremental" },
  { component: "Quality", safe: "dbt tests + Great Expectations", bleeding: "SQLMesh audits + Soda", why: "ML anomaly detection, built-in audit framework" },
  { component: "BI", safe: "Preset / Looker dashboards", bleeding: "LLM agents + Evidence.dev", why: "Natural language analytics, code-first reports" },
  { component: "Data Science", safe: "Pandas + PyAthena", bleeding: "Polars + DuckDB (zero-copy)", why: "10-100× faster than Pandas, no serialization" },
];

const costEstimate = [
  { service: "S3 Storage", cost: "~$23/TB/month", notes: "Parquet data files only (no metadata files!)" },
  { service: "RDS PostgreSQL", cost: "~$15/month", notes: "DuckLake catalog (db.t4g.micro)" },
  { service: "MotherDuck", cost: "$0–250/month", notes: "Free tier or Business plan" },
  { service: "dlt", cost: "Free", notes: "Open source" },
  { service: "SQLMesh", cost: "Free", notes: "Open source (Tobiko Cloud is paid)" },
  { service: "Dagster", cost: "Free / $$$", notes: "Open source or Dagster Cloud" },
  { service: "Cube.dev", cost: "Free / $$$", notes: "Open source or Cube Cloud" },
  { service: "Soda Core", cost: "Free", notes: "Open source (Soda Cloud is paid)" },
];

const risks = [
  { risk: "DuckLake ecosystem", detail: "Fewer engine integrations than Iceberg. Spark/Trino connectors in progress but not production-ready yet.", mitigation: "DuckLake 0.3 exports to Iceberg — you can migrate if needed." },
  { risk: "MotherDuck pricing", detail: "Lite plan removed, Business jumped to $250/month. Pricing has changed 3 times in 18 months.", mitigation: "Self-host DuckDB on ECS as fallback. Your data is on S3 — you're not locked in." },
  { risk: "SQLMesh maturity", detail: "Smaller community than dbt. Fewer tutorials, blog posts, and Stack Overflow answers.", mitigation: "SQLMesh imports dbt projects. You can migrate back if needed." },
  { risk: "Single-node limits", detail: "DuckDB won't handle 50TB+ scans or 100+ concurrent heavy users.", mitigation: "Export DuckLake → Iceberg → Spark/Trino when you hit the ceiling." },
  { risk: "LLM reliability", detail: "AI agents can hallucinate metrics or misinterpret questions.", mitigation: "Cube semantic layer constrains the LLM to defined metrics — can't make up numbers." },
];

export default function BleedingEdgeStack() {
  const [activeLayer, setActiveLayer] = useState(null);
  const [activeTab, setActiveTab] = useState("overview");
  const active = layers.find((l) => l.id === activeLayer);

  return (
    <div style={{
      fontFamily: "'IBM Plex Mono', monospace",
      background: "#07070d",
      color: "#e2e2e8",
      minHeight: "100vh",
      padding: "24px",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@300;400;500;600;700&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        ::-webkit-scrollbar { width: 5px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #333; border-radius: 3px; }
        pre { white-space: pre-wrap; word-wrap: break-word; }
      `}</style>

      {/* Header */}
      <div style={{ marginBottom: 28, paddingBottom: 20, borderBottom: "1px solid #1a1a24" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
          <span style={{
            fontSize: 9, letterSpacing: 3, color: "#000",
            background: "#f43f5e", padding: "3px 8px", borderRadius: 2,
            fontWeight: 700,
          }}>BLEEDING EDGE</span>
          <span style={{ fontSize: 10, letterSpacing: 3, color: "#555" }}>APRIL 2026</span>
        </div>
        <h1 style={{
          fontSize: 26, fontWeight: 700, color: "#fff",
          fontFamily: "'IBM Plex Sans', sans-serif", letterSpacing: -0.5,
        }}>
          Most Modern Data Engineering Stack
        </h1>
        <div style={{
          fontSize: 13, color: "#f43f5e", marginTop: 8,
          fontFamily: "'IBM Plex Mono', monospace", lineHeight: 1.6,
        }}>
          dlt → S3 + DuckLake (PostgreSQL) → SQLMesh + DuckDB/MotherDuck → S3 → Cube.dev → LLM Agents
        </div>
        <div style={{ fontSize: 12, color: "#555", marginTop: 4 }}>
          No Iceberg. No Glue. No Athena. No dbt.
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 0, marginBottom: 24, borderBottom: "1px solid #1a1a24" }}>
        {["overview", "vs safe", "costs", "risks"].map((tab) => (
          <button
            key={tab}
            onClick={() => { setActiveTab(tab); if (tab !== "overview") setActiveLayer(null); }}
            style={{
              padding: "8px 18px", background: "none", border: "none",
              borderBottom: activeTab === tab ? "2px solid #f43f5e" : "2px solid transparent",
              color: activeTab === tab ? "#fff" : "#555", cursor: "pointer",
              fontSize: 11, fontFamily: "'IBM Plex Mono', monospace",
              letterSpacing: 1, textTransform: "uppercase",
            }}
          >
            {tab}
          </button>
        ))}
      </div>

      {activeTab === "vs safe" ? (
        <div style={{ maxWidth: 800 }}>
          <div style={{ fontSize: 10, letterSpacing: 3, color: "#555", marginBottom: 16 }}>BLEEDING EDGE vs PRODUCTION-SAFE</div>
          {comparison.map((item, i) => (
            <div key={i} style={{
              display: "grid", gridTemplateColumns: "130px 1fr 1fr 1fr",
              padding: "12px 0", borderBottom: "1px solid #111",
              fontSize: 12, fontFamily: "'IBM Plex Sans', sans-serif", lineHeight: 1.5,
              alignItems: "start",
            }}>
              <span style={{ color: "#888", fontFamily: "'IBM Plex Mono', monospace", fontSize: 11 }}>{item.component}</span>
              <span style={{ color: "#666", paddingRight: 12 }}>{item.safe}</span>
              <span style={{ color: "#f43f5e", paddingRight: 12 }}>{item.bleeding}</span>
              <span style={{ color: "#555", fontSize: 11 }}>{item.why}</span>
            </div>
          ))}
        </div>
      ) : activeTab === "costs" ? (
        <div style={{ maxWidth: 700 }}>
          <div style={{ fontSize: 10, letterSpacing: 3, color: "#555", marginBottom: 16 }}>MONTHLY COST ESTIMATE</div>
          {costEstimate.map((item, i) => (
            <div key={i} style={{
              display: "grid", gridTemplateColumns: "160px 140px 1fr",
              padding: "10px 0", borderBottom: "1px solid #111",
              fontSize: 13, fontFamily: "'IBM Plex Sans', sans-serif",
            }}>
              <span style={{ color: "#ccc" }}>{item.service}</span>
              <span style={{ color: "#4ade80", fontFamily: "'IBM Plex Mono', monospace", fontSize: 12 }}>{item.cost}</span>
              <span style={{ color: "#666" }}>{item.notes}</span>
            </div>
          ))}
          <div style={{ marginTop: 24, padding: 16, background: "#0d0d14", borderRadius: 6, border: "1px solid #1a1a24" }}>
            <div style={{ fontSize: 13, color: "#ccc", fontFamily: "'IBM Plex Sans', sans-serif", lineHeight: 1.7 }}>
              <strong style={{ color: "#4ade80" }}>Bottom line:</strong> For a small team with &lt;5TB, this stack runs on <strong style={{ color: "#fff" }}>$40–300/month</strong> total AWS spend. The open-source tools are free. If you use MotherDuck Free tier + self-host Cube/Dagster, the floor is ~$40/month (S3 + RDS).
            </div>
          </div>
        </div>
      ) : activeTab === "risks" ? (
        <div style={{ maxWidth: 750 }}>
          <div style={{ fontSize: 10, letterSpacing: 3, color: "#555", marginBottom: 16 }}>RISKS & MITIGATIONS</div>
          {risks.map((item, i) => (
            <div key={i} style={{
              padding: "16px 0", borderBottom: "1px solid #111",
              fontFamily: "'IBM Plex Sans', sans-serif",
            }}>
              <div style={{ fontSize: 14, color: "#f43f5e", fontWeight: 500, marginBottom: 4 }}>{item.risk}</div>
              <div style={{ fontSize: 12, color: "#888", lineHeight: 1.6, marginBottom: 6 }}>{item.detail}</div>
              <div style={{ fontSize: 12, color: "#4ade80", lineHeight: 1.6 }}>↳ {item.mitigation}</div>
            </div>
          ))}
        </div>
      ) : (
        <div style={{ display: "flex", gap: 24, minHeight: 500 }}>
          {/* Left: Layer list */}
          <div style={{ width: 300, flexShrink: 0 }}>
            {layers.map((layer) => (
              <button
                key={layer.id}
                onClick={() => setActiveLayer(activeLayer === layer.id ? null : layer.id)}
                style={{
                  display: "flex", alignItems: "center", gap: 12, width: "100%",
                  padding: "11px 12px", marginBottom: 2,
                  background: activeLayer === layer.id ? "#0d0d18" : "transparent",
                  border: "1px solid",
                  borderColor: activeLayer === layer.id ? layer.color + "44" : "transparent",
                  borderRadius: 5, cursor: "pointer", textAlign: "left",
                }}
              >
                <div style={{
                  width: 30, height: 30, borderRadius: 4,
                  background: layer.color + "15", color: layer.color,
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 14, flexShrink: 0,
                }}>{layer.icon}</div>
                <div>
                  <div style={{ fontSize: 12, fontWeight: 500, color: activeLayer === layer.id ? "#fff" : "#bbb", fontFamily: "'IBM Plex Sans', sans-serif" }}>
                    {layer.title}
                  </div>
                  <div style={{ fontSize: 10, color: "#555" }}>{layer.subtitle}</div>
                </div>
              </button>
            ))}

            <div style={{ marginTop: 16, padding: 14, background: "#0a0a12", borderRadius: 6, border: "1px solid #151520" }}>
              <div style={{ fontSize: 9, letterSpacing: 2, color: "#444", marginBottom: 10 }}>FLOW</div>
              {layers.map((layer, i) => (
                <div key={layer.id}>
                  <div style={{ display: "flex", alignItems: "center", gap: 7, padding: "3px 0" }}>
                    <div style={{ width: 6, height: 6, borderRadius: "50%", background: layer.color, boxShadow: `0 0 8px ${layer.color}33` }} />
                    <span style={{ fontSize: 10, color: "#777" }}>{layer.title.split("—")[1]?.trim() || layer.subtitle}</span>
                  </div>
                  {i < layers.length - 1 && <div style={{ width: 1, height: 10, background: "#222", marginLeft: 3 }} />}
                </div>
              ))}
            </div>
          </div>

          {/* Right: Detail */}
          <div style={{ flex: 1, minWidth: 0 }}>
            {!active ? (
              <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "#333", fontSize: 13, fontFamily: "'IBM Plex Sans', sans-serif" }}>
                ← Select a layer to explore
              </div>
            ) : (
              <div key={active.id} style={{ animation: "fadeIn 0.15s ease" }}>
                <style>{`@keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }`}</style>
                <div style={{ fontSize: 18, fontWeight: 600, color: "#fff", fontFamily: "'IBM Plex Sans', sans-serif", marginBottom: 3 }}>{active.title}</div>
                <div style={{ fontSize: 11, color: active.color, marginBottom: 20 }}>{active.subtitle}</div>

                <div style={{ marginBottom: 22 }}>
                  <div style={{ fontSize: 9, letterSpacing: 3, color: "#444", marginBottom: 6 }}>WHAT</div>
                  <p style={{ fontSize: 13, lineHeight: 1.7, color: "#aaa", fontFamily: "'IBM Plex Sans', sans-serif" }}>{active.details.what}</p>
                </div>

                <div style={{ marginBottom: 22 }}>
                  <div style={{ fontSize: 9, letterSpacing: 3, color: "#444", marginBottom: 6 }}>WHY</div>
                  {active.details.why.map((item, i) => (
                    <div key={i} style={{ display: "flex", gap: 7, marginBottom: 5, fontSize: 12, lineHeight: 1.6, fontFamily: "'IBM Plex Sans', sans-serif" }}>
                      <span style={{ color: active.color, flexShrink: 0 }}>›</span>
                      <span style={{ color: "#888" }}>{item}</span>
                    </div>
                  ))}
                </div>

                <div style={{ marginBottom: 22 }}>
                  <div style={{ fontSize: 9, letterSpacing: 3, color: "#444", marginBottom: 6 }}>CODE</div>
                  <pre style={{
                    background: "#08080e", border: "1px solid #151520", borderRadius: 5,
                    padding: 14, fontSize: 10.5, lineHeight: 1.6, color: "#7a7a90",
                    overflow: "auto", maxHeight: 380,
                  }}>{active.details.how}</pre>
                </div>

                <div>
                  <div style={{ fontSize: 9, letterSpacing: 3, color: "#444", marginBottom: 6 }}>CONFIG</div>
                  {Object.entries(active.details.config).map(([key, val]) => (
                    <div key={key} style={{
                      display: "grid", gridTemplateColumns: "170px 1fr",
                      padding: "7px 0", borderBottom: "1px solid #0e0e16", fontSize: 11,
                    }}>
                      <span style={{ color: "#666", fontFamily: "'IBM Plex Mono', monospace" }}>{key}</span>
                      <span style={{ color: "#aaa", fontFamily: "'IBM Plex Sans', sans-serif", lineHeight: 1.5 }}>{val}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
