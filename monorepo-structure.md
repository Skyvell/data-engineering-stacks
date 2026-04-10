# Bleeding Edge Data Stack — Monorepo Structure

```
data-platform/
│
├── README.md                          # Stack overview, setup guide, architecture diagram
├── Makefile                           # Common commands: make setup, make dev, make test, make deploy
├── pyproject.toml                     # Single Python project — all deps managed here
├── .env.example                       # Template for env vars (never commit .env)
├── .github/
│   └── workflows/
│       ├── ci.yml                     # On PR: lint + sqlmesh plan + soda scan + unit tests
│       ├── deploy.yml                 # On merge to main: sqlmesh apply + dagster deploy
│       └── scheduled-quality.yml      # Daily: full soda scan + freshness checks
│
│
│── ingestion/                         # ── LAYER 1: dlt pipelines ──
│   ├── __init__.py
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── stripe.py                  # dlt source: Stripe API
│   │   ├── hubspot.py                 # dlt source: HubSpot API
│   │   ├── postgres_cdc.py            # Sling config for Postgres CDC
│   │   └── google_sheets.py           # dlt source: Google Sheets
│   ├── helpers/
│   │   ├── __init__.py
│   │   ├── pagination.py              # Shared pagination logic
│   │   └── rate_limiting.py           # Shared rate limiting
│   ├── schemas/                       # dlt schema evolution configs
│   │   ├── stripe.schema.yaml
│   │   └── hubspot.schema.yaml
│   └── tests/
│       ├── test_stripe.py             # Unit tests with sample API responses
│       └── test_hubspot.py
│
│
├── transform/                         # ── LAYER 2: SQLMesh project ──
│   ├── config.yaml                    # SQLMesh config (DuckDB/MotherDuck connection)
│   ├── models/
│   │   ├── staging/                   # 1:1 with raw sources, rename + cast + filter
│   │   │   ├── stg_stripe__payments.sql
│   │   │   ├── stg_stripe__customers.sql
│   │   │   ├── stg_hubspot__contacts.sql
│   │   │   └── stg_hubspot__deals.sql
│   │   ├── intermediate/              # Business logic joins, deduplication
│   │   │   ├── int_payments_enriched.sql
│   │   │   └── int_contacts_with_deals.sql
│   │   └── marts/                     # Final business entities — consumed by Cube
│   │       ├── fct_revenue.sql
│   │       ├── fct_deals.sql
│   │       ├── dim_customers.sql
│   │       └── dim_products.sql
│   ├── audits/                        # Data quality checks (run on every plan/apply)
│   │   ├── assert_revenue_positive.sql
│   │   ├── assert_no_orphan_payments.sql
│   │   └── assert_customer_email_valid.sql
│   ├── macros/
│   │   ├── cents_to_dollars.sql
│   │   ├── safe_divide.sql
│   │   └── date_spine.sql
│   ├── seeds/
│   │   ├── country_codes.csv
│   │   └── currency_exchange_rates.csv
│   └── tests/                         # SQLMesh unit tests
│       └── test_fct_revenue.yaml
│
│
├── quality/                           # ── LAYER 3: Soda checks ──
│   ├── soda_config.yml                # Soda connection to DuckDB/MotherDuck
│   ├── checks/
│   │   ├── raw/
│   │   │   ├── orders.yml             # Freshness, row count, anomaly detection
│   │   │   └── customers.yml
│   │   ├── staging/
│   │   │   └── stg_stripe__payments.yml
│   │   └── marts/
│   │       ├── fct_revenue.yml        # Anomaly detection on revenue metrics
│   │       └── dim_customers.yml
│   └── scripts/
│       └── run_scan.py                # Helper to run Soda scans programmatically
│
│
├── orchestration/                     # ── LAYER 4: Dagster ──
│   ├── __init__.py
│   ├── definitions.py                 # Main Dagster definitions (entry point)
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── ingestion.py               # dlt assets (wraps ingestion/sources/*)
│   │   ├── transformation.py          # SQLMesh assets (wraps transform/)
│   │   └── quality.py                 # Soda scan assets (wraps quality/)
│   ├── resources.py                   # Shared resources: DuckDB conn, S3, secrets
│   ├── schedules.py                   # Cron schedules for daily/hourly runs
│   ├── sensors.py                     # S3 file arrival sensors, webhook sensors
│   ├── partitions.py                  # Daily/weekly partition definitions
│   └── tests/
│       └── test_definitions.py        # Dagster unit tests
│
│
├── semantic/                          # ── LAYER 5: Cube.dev ──
│   ├── cube.js                        # Cube config (DuckDB/MotherDuck data source)
│   ├── schema/
│   │   ├── Revenue.js                 # Revenue metrics cube
│   │   ├── Customers.js               # Customer dimension cube
│   │   ├── Deals.js                   # Deals metrics cube
│   │   └── Products.js                # Product dimension cube
│   ├── .env.example                   # Cube env vars (DB connection, JWT secret)
│   └── Dockerfile                     # Cube deployment container
│
│
├── infra/                             # ── INFRASTRUCTURE ──
│   ├── terraform/                     # or Pulumi/CDK — your choice
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   ├── modules/
│   │   │   ├── s3/                    # Data lake bucket + lifecycle rules
│   │   │   │   └── main.tf
│   │   │   ├── rds/                   # PostgreSQL for DuckLake catalog
│   │   │   │   └── main.tf
│   │   │   ├── ecs/                   # ECS tasks for Dagster + Cube
│   │   │   │   └── main.tf
│   │   │   ├── iam/                   # IAM roles and policies
│   │   │   │   └── main.tf
│   │   │   └── networking/            # VPC, subnets, security groups
│   │   │       └── main.tf
│   │   └── environments/
│   │       ├── dev.tfvars
│   │       ├── staging.tfvars
│   │       └── prod.tfvars
│   └── docker/
│       ├── dagster.Dockerfile         # Dagster webserver + daemon
│       ├── cube.Dockerfile            # Cube.dev API server
│       └── docker-compose.yml         # Local dev: Dagster + Cube + Postgres
│
│
├── scripts/                           # ── UTILITY SCRIPTS ──
│   ├── setup_ducklake.py              # Initialize DuckLake: create schemas, attach catalog
│   ├── seed_dev_data.py               # Load sample data for local development
│   ├── run_backfill.py                # Backfill historical data via dlt
│   ├── export_to_iceberg.py           # Escape hatch: DuckLake → Iceberg migration
│   └── health_check.py               # Verify all connections (S3, RDS, MotherDuck)
│
│
└── docs/                              # ── DOCUMENTATION ──
    ├── architecture.md                # Architecture diagram + decisions
    ├── runbook.md                     # Incident response: what to do when things break
    ├── onboarding.md                  # New engineer setup guide
    ├── data-dictionary.md             # Business definitions for all mart tables
    └── adr/                           # Architecture Decision Records
        ├── 001-ducklake-over-iceberg.md
        ├── 002-sqlmesh-over-dbt.md
        ├── 003-motherduck-over-athena.md
        └── 004-dagster-over-airflow.md
```

---

## Key Design Decisions

### Why monorepo?
- **One PR to change everything**: add a dlt source + SQLMesh model + Soda check + Dagster asset in a single PR
- **Shared Python environment**: all tools share the same `pyproject.toml` — no version conflicts
- **Single CI pipeline**: lint, test, plan, and deploy from one workflow
- **Easy local dev**: `make dev` starts everything you need

### Why this folder structure?
- **Separation by concern, not by tool**: each folder is a layer of the stack, not a tool config directory
- **`ingestion/`** owns everything about getting data in — dlt sources, Sling configs, schemas
- **`transform/`** is a standalone SQLMesh project — you can `cd transform && sqlmesh plan` independently
- **`quality/`** is separate from transforms — Soda checks run post-pipeline, not inline
- **`orchestration/`** ties everything together but doesn't own business logic
- **`semantic/`** is its own deployable service (Cube runs as a separate container)
- **`infra/`** is fully separated — data engineers don't need to touch Terraform daily

---

## pyproject.toml

```toml
[project]
name = "data-platform"
version = "0.1.0"
requires-python = ">=3.11"

dependencies = [
    # Ingestion
    "dlt[motherduck]>=1.0",
    
    # Transformation
    "sqlmesh[duckdb]>=0.90",
    
    # Quality
    "soda-core-duckdb>=3.0",
    
    # Orchestration
    "dagster>=1.7",
    "dagster-webserver>=1.7",
    "dagster-dlt>=0.24",
    "dagster-sqlmesh>=0.2",
    
    # Shared
    "duckdb>=1.4",
    "polars>=1.0",
    "boto3>=1.34",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "ruff>=0.5",
    "pre-commit>=3.7",
]

[tool.ruff]
line-length = 120
target-version = "py311"
```

---

## Makefile

```makefile
.PHONY: setup dev test lint plan apply quality deploy

# First-time setup
setup:
	pip install -e ".[dev]"
	pre-commit install
	python scripts/setup_ducklake.py
	python scripts/seed_dev_data.py

# Start local dev environment (Dagster UI + Postgres + Cube)
dev:
	docker compose -f infra/docker/docker-compose.yml up -d postgres
	cd orchestration && dagster dev

# Run all tests
test:
	pytest ingestion/tests/ -v
	pytest orchestration/tests/ -v
	cd transform && sqlmesh test

# Lint
lint:
	ruff check .
	ruff format --check .

# SQLMesh plan (preview changes)
plan:
	cd transform && sqlmesh plan

# SQLMesh apply (execute changes)
apply:
	cd transform && sqlmesh apply

# Run Soda quality checks
quality:
	soda scan -d motherduck -c quality/soda_config.yml quality/checks/

# Deploy to production
deploy:
	cd transform && sqlmesh plan prod --auto-apply
	dagster-cloud ci deploy
```

---

## CI/CD Pipeline (.github/workflows/ci.yml)

```yaml
name: CI

on:
  pull_request:
    branches: [main]

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[dev]"
      - run: ruff check .
      - run: pytest ingestion/tests/ -v

  sqlmesh-plan:
    runs-on: ubuntu-latest
    needs: lint-and-test
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[dev]"
      - run: cd transform && sqlmesh plan --auto-categorize
        env:
          MOTHERDUCK_TOKEN: ${{ secrets.MOTHERDUCK_TOKEN }}
      # SQLMesh posts a plan summary as a PR comment

  soda-scan:
    runs-on: ubuntu-latest
    needs: sqlmesh-plan
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[dev]"
      - run: soda scan -d motherduck -c quality/soda_config.yml quality/checks/
        env:
          MOTHERDUCK_TOKEN: ${{ secrets.MOTHERDUCK_TOKEN }}
```

---

## Local Development Flow

```bash
# 1. Clone and setup
git clone git@github.com:yourorg/data-platform.git
cd data-platform
make setup

# 2. Start local dev
make dev
# → Dagster UI at http://localhost:3000
# → Postgres (DuckLake catalog) at localhost:5432

# 3. Add a new source
#    Edit ingestion/sources/new_source.py
#    Edit orchestration/assets/ingestion.py
#    Run: dagster dev  →  trigger manually in UI

# 4. Add a new model
#    Edit transform/models/staging/stg_new_source.sql
#    Run: make plan  →  see column-level diff
#    Run: make apply  →  execute in virtual dev environment

# 5. Add quality checks
#    Edit quality/checks/staging/stg_new_source.yml
#    Run: make quality

# 6. Open PR → CI runs plan + tests + soda
# 7. Merge → CD applies to prod
```

---

## Environment Strategy

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Local Dev  │     │   Staging   │     │  Production  │
│              │     │             │     │              │
│  DuckDB      │     │ MotherDuck  │     │ MotherDuck   │
│  (in-process)│     │ (dev db)    │     │ (prod db)    │
│              │     │             │     │              │
│  Local       │     │ RDS         │     │ RDS          │
│  Postgres    │     │ Postgres    │     │ Postgres     │
│  (Docker)    │     │ (dev)       │     │ (prod)       │
│              │     │             │     │              │
│  Local S3    │     │ S3          │     │ S3           │
│  (MinIO or   │     │ dev bucket  │     │ prod bucket  │
│   localstack)│     │             │     │              │
└─────────────┘     └─────────────┘     └─────────────┘

SQLMesh virtual environments handle dev/staging/prod
without duplicating data. Only prod actually materializes.
```
