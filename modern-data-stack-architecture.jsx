import { useState } from "react";

const layers = [
  {
    id: "ingestion",
    title: "1. Ingestion — dlt",
    subtitle: "Data Load Tool",
    color: "#2563eb",
    icon: "↓",
    details: {
      what: "Python-native ELT framework that extracts data from APIs, databases, and files, then loads it directly into your data lake as Iceberg tables.",
      why: [
        "No separate infrastructure — it's just a Python library (pip install dlt)",
        "Automatic schema inference and evolution on write",
        "Built-in Iceberg destination support with Glue Catalog sync",
        "Handles pagination, rate limiting, retries, incremental loading out of the box",
        "Version-controlled pipelines — your ingestion is just Python code in Git",
        "Free and open source — no per-connector licensing like Fivetran",
      ],
      how: `import dlt

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
pipeline.run(my_api())`,
      config: {
        "S3 Bucket": "Your data lake bucket (e.g. s3://my-datalake/)",
        "Glue Catalog": "dlt registers tables automatically in Glue",
        "Write Disposition": "append, replace, or merge (SCD Type 1/2)",
        "Incremental": "Supports cursor-based and merge-based incremental loads",
        "File Format": "Parquet by default (optimal for Iceberg)",
        "Parallelism": "Configurable worker count for concurrent extraction",
      },
      sources: "200+ verified sources: Salesforce, Stripe, PostgreSQL, MySQL, REST APIs, Google Sheets, S3 files, MongoDB, HubSpot, GitHub, Slack, etc.",
    },
  },
  {
    id: "storage",
    title: "2. Storage — S3 + Apache Iceberg",
    subtitle: "Lakehouse Foundation",
    color: "#059669",
    icon: "◆",
    details: {
      what: "All data lives as Parquet files on S3, organized by the Iceberg table format. Iceberg adds a metadata layer that gives you warehouse-like features on cheap object storage.",
      why: [
        "Storage and compute fully decoupled — pay S3 rates ($0.023/GB/month), not warehouse rates",
        "ACID transactions — no partial writes or corrupted reads",
        "Schema evolution — add/rename/drop columns without rewriting data",
        "Time travel — query data as it existed at any point in time",
        "Partition evolution — change partitioning strategy without rewriting",
        "Hidden partitioning — no need to include partition columns in queries",
        "Engine-agnostic — Athena, Spark, Trino, DuckDB all read the same tables",
      ],
      how: `-- S3 Structure:
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
  └── _dagster/                   # Dagster metadata`,
      config: {
        "S3 Bucket Config": "Versioning enabled, lifecycle rules for old Iceberg snapshots",
        "Iceberg Format Version": "v2 (supports row-level deletes, equality deletes)",
        "File Format": "Parquet with Snappy compression",
        "Partition Strategy": "Start unpartitioned, add partitioning when tables grow large",
        "Snapshot Expiry": "Configure to expire old snapshots (e.g. keep 7 days)",
        "Compaction": "Schedule regular compaction to merge small files",
        "Estimated Cost": "~$23/TB/month for storage + minimal API costs",
      },
      sources: null,
    },
  },
  {
    id: "catalog",
    title: "3. Catalog — AWS Glue Catalog",
    subtitle: "Metadata Registry",
    color: "#7c3aed",
    icon: "☰",
    details: {
      what: "A managed Hive-compatible metastore that acts as a phone book for your Iceberg tables. When Athena or Spark asks 'where is the orders table?', Glue Catalog answers.",
      why: [
        "Fully managed — no servers, no Hive Metastore to operate",
        "Native AWS integration — Athena, EMR, Spark all use it natively",
        "Stores database/table/partition metadata",
        "Cheap — first million objects free, then $1 per 100K objects/month",
        "dlt and dbt-athena both sync to it automatically",
      ],
      how: `-- Glue Catalog hierarchy:
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

-- Each table entry contains:
--   • S3 location
--   • Column names, types
--   • Iceberg metadata pointer
--   • Table properties (format, compression, etc.)`,
      config: {
        "IAM Permissions": "glue:GetTable, glue:GetDatabase, glue:CreateTable, glue:UpdateTable",
        "Database Naming": "One Glue database per dbt schema (raw, staging, marts)",
        "Table Type": "EXTERNAL_TABLE with table_type=ICEBERG",
        "Lake Formation": "Skip unless you need row/column-level security across teams",
        "Crawler": "Not needed — dlt and dbt register tables directly",
        "Cost": "Effectively free for most workloads",
      },
      sources: null,
    },
  },
  {
    id: "compute",
    title: "4. Compute — AWS Athena",
    subtitle: "Serverless Query Engine",
    color: "#dc2626",
    icon: "⚡",
    details: {
      what: "Serverless SQL engine that executes your dbt transformations. It reads Iceberg tables from S3 via Glue Catalog, processes the SQL, and writes results back as new Iceberg tables on S3.",
      why: [
        "Fully serverless — no clusters to provision, start, stop, or scale",
        "Pay per query — $5 per TB scanned (no idle costs)",
        "Native Iceberg support — reads metadata to skip irrelevant files (predicate pushdown)",
        "Supports dbt via dbt-athena-community adapter",
        "Supports CTAS (Create Table As Select) for materialized dbt models",
        "Good for small-to-mid workloads (up to ~TB/day of transforms)",
      ],
      how: `# dbt project config (profiles.yml):
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

# dbt model example (marts/fct_revenue.sql):
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
GROUP BY 1, 2, 3`,
      config: {
        "Workgroup": "Create separate workgroups for dbt vs ad-hoc queries",
        "Query Result Location": "s3://my-datalake/athena-results/ (temp results)",
        "Encryption": "SSE-S3 or SSE-KMS for results at rest",
        "Cost Control": "Set per-query data scan limits in workgroup settings",
        "Engine Version": "Athena v3 (Trino-based, best Iceberg support)",
        "When to Migrate": "If monthly bill exceeds ~$500-1000, evaluate EMR Serverless",
        "Migration Path": "Swap dbt adapter from athena to spark, everything else stays",
      },
      sources: null,
    },
  },
  {
    id: "transform",
    title: "5. Transformation — dbt",
    subtitle: "Data Build Tool",
    color: "#ea580c",
    icon: "⟳",
    details: {
      what: "SQL-based transformation framework. You write SELECT statements, dbt compiles them, orders them by dependency, and executes them against Athena. Results are materialized as new Iceberg tables.",
      why: [
        "Industry standard for SQL transformations",
        "Dependency management — dbt figures out execution order from ref() calls",
        "Incremental models — process only new/changed data",
        "Built-in testing — not null, unique, relationships, custom tests",
        "Auto-generated documentation and lineage graphs",
        "Version-controlled — all models live in Git",
        "Jinja templating for DRY SQL",
      ],
      how: `# Project structure:
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

# Staging model (models/staging/stg_orders.sql):
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
SELECT * FROM renamed`,
      config: {
        "Materialization": "view (staging), incremental (facts), table (dimensions)",
        "dbt Adapter": "dbt-athena-community (pip install dbt-athena-community)",
        "Testing": "Configure not_null, unique, accepted_values, relationships in YAML",
        "Naming Convention": "stg_ (staging), int_ (intermediate), fct_ (facts), dim_ (dimensions)",
        "Environments": "dev (personal schema), staging (CI), prod (production)",
        "CI/CD": "Run dbt build on PR → test in staging schema → merge → deploy to prod",
      },
      sources: null,
    },
  },
  {
    id: "orchestration",
    title: "6. Orchestration — Dagster",
    subtitle: "Asset-Based Orchestration",
    color: "#0891b2",
    icon: "◎",
    details: {
      what: "Orchestrator that schedules and monitors your entire pipeline. Unlike Airflow (task-based), Dagster thinks in assets — each dlt source and dbt model is a data asset with dependencies, freshness expectations, and observability.",
      why: [
        "Asset-based — maps naturally to dlt sources and dbt models",
        "First-class dbt integration — auto-imports dbt models as Dagster assets",
        "First-class dlt integration — dagster-dlt package",
        "Built-in asset lineage, freshness policies, and auto-materialization",
        "Local dev experience is excellent (dagster dev command)",
        "Dagster Cloud available for managed deployment",
        "Sensors and schedules for event-driven or cron-based runs",
      ],
      how: `# Dagster project structure:
my_dagster_project/
  ├── definitions.py
  ├── assets/
  │   ├── ingestion.py      # dlt assets
  │   └── transformation.py  # dbt assets
  ├── resources.py
  └── schedules.py

# assets/ingestion.py
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt import pipeline

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
)`,
      config: {
        "Deployment": "Dagster Cloud (managed) or self-hosted on ECS/K8s",
        "dagster-dlt": "pip install dagster-dlt (official integration)",
        "dagster-dbt": "pip install dagster-dbt (official integration)",
        "Freshness Policies": "Define SLAs per asset (e.g. orders must be <6 hours old)",
        "Auto-Materialization": "Dagster can auto-trigger downstream when upstream refreshes",
        "Sensors": "Trigger pipelines on S3 file arrival, API webhooks, etc.",
        "Partitions": "Supports daily/weekly/monthly partitioned assets natively",
      },
      sources: null,
    },
  },
  {
    id: "semantic",
    title: "7. Semantic Layer — Cube.dev",
    subtitle: "Metrics & API Layer",
    color: "#c026d3",
    icon: "▣",
    details: {
      what: "Defines your business metrics (revenue, churn, ARR, etc.) once in code, then exposes them via REST/GraphQL API to any consumer — BI tools, notebooks, LLM agents, internal apps. Single source of truth for metrics.",
      why: [
        "Define metrics once, use everywhere — no more conflicting definitions across tools",
        "Built-in caching and pre-aggregation — fast queries without hitting Athena every time",
        "REST and GraphQL APIs — any app or LLM can query your metrics",
        "Supports Athena as a data source natively",
        "Access control — define who sees what data",
        "Open source core with managed cloud option",
      ],
      how: `// cube/schema/Revenue.js
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
      sql: \`\${total_revenue} / NULLIF(\${order_count}, 0)\`,
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

// API query example:
// GET /cubejs-api/v1/load?query={
//   "measures": ["Revenue.total_revenue"],
//   "timeDimensions": [{
//     "dimension": "Revenue.order_date",
//     "granularity": "month"
//   }],
//   "filters": [{
//     "member": "Revenue.customer_segment",
//     "operator": "equals",
//     "values": ["Enterprise"]
//   }]
// }`,
      config: {
        "Data Source": "Athena (configured via environment variables)",
        "Caching": "In-memory + pre-aggregations stored as Parquet on S3",
        "Deployment": "Docker container on ECS, or Cube Cloud (managed)",
        "Auth": "JWT-based, integrates with your identity provider",
        "BI Integration": "Native connectors for Preset, Metabase, Streamlit, Grafana",
        "LLM Integration": "REST API is perfect for AI agents to query structured metrics",
        "Refresh": "Configurable refresh keys per pre-aggregation",
      },
      sources: null,
    },
  },
  {
    id: "consumers",
    title: "8. Consumers",
    subtitle: "BI, Apps & AI Agents",
    color: "#475569",
    icon: "◈",
    details: {
      what: "Everything that consumes your data: BI dashboards, internal apps, data science notebooks, LLM-powered agents. All query through Cube's API or directly against Athena.",
      why: [
        "Cube API gives every consumer consistent metrics",
        "BI tools (Preset/Superset, Lightdash, Metabase) connect to Cube or Athena",
        "Data science notebooks (Jupyter) can query Athena via PyAthena",
        "LLM agents can call Cube's REST API for structured analytics",
        "Internal apps use Cube's GraphQL/REST for embedded analytics",
      ],
      how: `# Python notebook querying Athena directly:
import pyathena
conn = pyathena.connect(
    s3_staging_dir="s3://my-datalake/athena-results/",
    region_name="eu-north-1"
)
df = pd.read_sql("SELECT * FROM marts.fct_revenue", conn)

# LLM agent querying Cube API:
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

# Preset/Superset connects via:
#   Athena connection string or Cube SQL API`,
      config: {
        "BI Tool": "Preset (hosted Superset) or Lightdash for open-source-friendly option",
        "Notebooks": "Jupyter + PyAthena for data science",
        "LLM Agents": "Cube REST API → structured data → LLM generates insights",
        "Embedded Analytics": "Cube's REST/GraphQL for building dashboards in your app",
        "Reverse ETL": "Census or Hightouch to push data back to SaaS tools",
        "Alerting": "Set up alerts on metric thresholds via Cube or your BI tool",
      },
      sources: null,
    },
  },
];

const costEstimate = [
  { service: "S3 Storage", cost: "~$23/TB/month", notes: "Your entire data lake" },
  { service: "Athena Queries", cost: "$5/TB scanned", notes: "Iceberg minimizes scans via metadata" },
  { service: "Glue Catalog", cost: "~$0", notes: "First 1M objects free" },
  { service: "dlt", cost: "Free", notes: "Open source Python library" },
  { service: "dbt Core", cost: "Free", notes: "Open source (dbt Cloud is paid)" },
  { service: "Dagster", cost: "Free / $$$", notes: "Open source or Dagster Cloud" },
  { service: "Cube.dev", cost: "Free / $$$", notes: "Open source or Cube Cloud" },
];

export default function ArchitectureDashboard() {
  const [activeLayer, setActiveLayer] = useState(null);
  const [activeTab, setActiveTab] = useState("overview");

  const active = layers.find((l) => l.id === activeLayer);

  return (
    <div style={{
      fontFamily: "'IBM Plex Mono', 'SF Mono', 'Fira Code', monospace",
      background: "#0a0a0f",
      color: "#e2e2e8",
      minHeight: "100vh",
      padding: "24px",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@300;400;500;600;700&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: #1a1a24; }
        ::-webkit-scrollbar-thumb { background: #333; border-radius: 3px; }
        pre { white-space: pre-wrap; word-wrap: break-word; }
      `}</style>

      {/* Header */}
      <div style={{ marginBottom: 32, borderBottom: "1px solid #222", paddingBottom: 20 }}>
        <div style={{ fontSize: 11, letterSpacing: 4, color: "#666", marginBottom: 6, fontFamily: "'IBM Plex Mono', monospace" }}>
          ARCHITECTURE REFERENCE
        </div>
        <h1 style={{
          fontSize: 28,
          fontWeight: 700,
          fontFamily: "'IBM Plex Sans', sans-serif",
          color: "#fff",
          letterSpacing: -0.5,
        }}>
          Modern Data Engineering Stack
        </h1>
        <div style={{ fontSize: 13, color: "#888", marginTop: 6, fontFamily: "'IBM Plex Sans', sans-serif" }}>
          dlt → S3 Iceberg → dbt + Athena → S3 Iceberg → Cube.dev → Consumers
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 0, marginBottom: 24, borderBottom: "1px solid #222" }}>
        {["overview", "costs"].map((tab) => (
          <button
            key={tab}
            onClick={() => { setActiveTab(tab); setActiveLayer(null); }}
            style={{
              padding: "8px 20px",
              background: "none",
              border: "none",
              borderBottom: activeTab === tab ? "2px solid #fff" : "2px solid transparent",
              color: activeTab === tab ? "#fff" : "#666",
              cursor: "pointer",
              fontSize: 12,
              fontFamily: "'IBM Plex Mono', monospace",
              letterSpacing: 1,
              textTransform: "uppercase",
            }}
          >
            {tab}
          </button>
        ))}
      </div>

      {activeTab === "costs" ? (
        <div style={{ maxWidth: 700 }}>
          <div style={{ fontSize: 11, letterSpacing: 3, color: "#666", marginBottom: 16 }}>MONTHLY COST ESTIMATE</div>
          {costEstimate.map((item, i) => (
            <div key={i} style={{
              display: "grid",
              gridTemplateColumns: "160px 140px 1fr",
              padding: "10px 0",
              borderBottom: "1px solid #1a1a24",
              fontSize: 13,
              fontFamily: "'IBM Plex Sans', sans-serif",
            }}>
              <span style={{ color: "#ccc" }}>{item.service}</span>
              <span style={{ color: "#4ade80", fontFamily: "'IBM Plex Mono', monospace", fontSize: 12 }}>{item.cost}</span>
              <span style={{ color: "#666" }}>{item.notes}</span>
            </div>
          ))}
          <div style={{ marginTop: 24, padding: 16, background: "#111118", borderRadius: 6, border: "1px solid #222" }}>
            <div style={{ fontSize: 13, color: "#ccc", fontFamily: "'IBM Plex Sans', sans-serif", lineHeight: 1.7 }}>
              <strong style={{ color: "#4ade80" }}>Bottom line:</strong> For a small-to-mid team processing &lt;1TB/day, your AWS bill will likely be <strong style={{ color: "#fff" }}>$50–200/month</strong>. The majority of the stack is open source and runs on minimal compute (a single ECS task for Dagster + Cube).
            </div>
          </div>
        </div>
      ) : (
        <div style={{ display: "flex", gap: 24, minHeight: 500 }}>
          {/* Left: Layer list */}
          <div style={{ width: 320, flexShrink: 0 }}>
            {layers.map((layer, i) => (
              <button
                key={layer.id}
                onClick={() => setActiveLayer(activeLayer === layer.id ? null : layer.id)}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 12,
                  width: "100%",
                  padding: "12px 14px",
                  marginBottom: 2,
                  background: activeLayer === layer.id ? "#141420" : "transparent",
                  border: "1px solid",
                  borderColor: activeLayer === layer.id ? layer.color + "44" : "transparent",
                  borderRadius: 6,
                  cursor: "pointer",
                  textAlign: "left",
                  transition: "all 0.15s",
                }}
              >
                <div style={{
                  width: 32,
                  height: 32,
                  borderRadius: 4,
                  background: layer.color + "18",
                  color: layer.color,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontSize: 16,
                  flexShrink: 0,
                }}>
                  {layer.icon}
                </div>
                <div>
                  <div style={{
                    fontSize: 13,
                    fontWeight: 500,
                    color: activeLayer === layer.id ? "#fff" : "#ccc",
                    fontFamily: "'IBM Plex Sans', sans-serif",
                  }}>
                    {layer.title}
                  </div>
                  <div style={{ fontSize: 11, color: "#666" }}>{layer.subtitle}</div>
                </div>
              </button>
            ))}

            {/* Flow diagram */}
            <div style={{
              marginTop: 20,
              padding: 16,
              background: "#111118",
              borderRadius: 6,
              border: "1px solid #1a1a24",
            }}>
              <div style={{ fontSize: 10, letterSpacing: 2, color: "#555", marginBottom: 12 }}>DATA FLOW</div>
              {layers.map((layer, i) => (
                <div key={layer.id}>
                  <div style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 8,
                    padding: "4px 0",
                  }}>
                    <div style={{
                      width: 8,
                      height: 8,
                      borderRadius: "50%",
                      background: layer.color,
                      boxShadow: `0 0 6px ${layer.color}44`,
                    }} />
                    <span style={{ fontSize: 11, color: "#999" }}>{layer.title.split("—")[1]?.trim() || layer.subtitle}</span>
                  </div>
                  {i < layers.length - 1 && (
                    <div style={{
                      width: 1,
                      height: 12,
                      background: "#333",
                      marginLeft: 4,
                    }} />
                  )}
                </div>
              ))}
            </div>
          </div>

          {/* Right: Detail panel */}
          <div style={{ flex: 1, minWidth: 0 }}>
            {!active ? (
              <div style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                height: "100%",
                color: "#444",
                fontSize: 13,
                fontFamily: "'IBM Plex Sans', sans-serif",
              }}>
                ← Select a layer to see details
              </div>
            ) : (
              <div style={{ animation: "fadeIn 0.2s ease" }}>
                <style>{`@keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }`}</style>

                <div style={{
                  fontSize: 20,
                  fontWeight: 600,
                  fontFamily: "'IBM Plex Sans', sans-serif",
                  color: "#fff",
                  marginBottom: 4,
                }}>
                  {active.title}
                </div>
                <div style={{ fontSize: 12, color: active.color, marginBottom: 20 }}>{active.subtitle}</div>

                {/* What */}
                <div style={{ marginBottom: 24 }}>
                  <div style={{ fontSize: 10, letterSpacing: 3, color: "#555", marginBottom: 8 }}>WHAT</div>
                  <p style={{ fontSize: 13, lineHeight: 1.7, color: "#bbb", fontFamily: "'IBM Plex Sans', sans-serif" }}>
                    {active.details.what}
                  </p>
                </div>

                {/* Why */}
                <div style={{ marginBottom: 24 }}>
                  <div style={{ fontSize: 10, letterSpacing: 3, color: "#555", marginBottom: 8 }}>WHY</div>
                  {active.details.why.map((item, i) => (
                    <div key={i} style={{
                      display: "flex",
                      gap: 8,
                      marginBottom: 6,
                      fontSize: 13,
                      lineHeight: 1.6,
                      fontFamily: "'IBM Plex Sans', sans-serif",
                    }}>
                      <span style={{ color: active.color, flexShrink: 0 }}>›</span>
                      <span style={{ color: "#999" }}>{item}</span>
                    </div>
                  ))}
                </div>

                {active.details.sources && (
                  <div style={{ marginBottom: 24, padding: 12, background: "#111118", borderRadius: 6, border: "1px solid #1a1a24" }}>
                    <div style={{ fontSize: 10, letterSpacing: 3, color: "#555", marginBottom: 6 }}>SOURCES</div>
                    <p style={{ fontSize: 12, color: "#888", fontFamily: "'IBM Plex Sans', sans-serif", lineHeight: 1.6 }}>
                      {active.details.sources}
                    </p>
                  </div>
                )}

                {/* Code */}
                <div style={{ marginBottom: 24 }}>
                  <div style={{ fontSize: 10, letterSpacing: 3, color: "#555", marginBottom: 8 }}>CODE / STRUCTURE</div>
                  <pre style={{
                    background: "#0d0d14",
                    border: "1px solid #1a1a24",
                    borderRadius: 6,
                    padding: 16,
                    fontSize: 11,
                    lineHeight: 1.6,
                    color: "#8b8ba0",
                    overflow: "auto",
                    maxHeight: 400,
                  }}>
                    {active.details.how}
                  </pre>
                </div>

                {/* Config */}
                <div>
                  <div style={{ fontSize: 10, letterSpacing: 3, color: "#555", marginBottom: 8 }}>CONFIGURATION</div>
                  {Object.entries(active.details.config).map(([key, val]) => (
                    <div key={key} style={{
                      display: "grid",
                      gridTemplateColumns: "180px 1fr",
                      padding: "8px 0",
                      borderBottom: "1px solid #1a1a20",
                      fontSize: 12,
                    }}>
                      <span style={{ color: "#888", fontFamily: "'IBM Plex Mono', monospace" }}>{key}</span>
                      <span style={{ color: "#bbb", fontFamily: "'IBM Plex Sans', sans-serif", lineHeight: 1.5 }}>{val}</span>
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
