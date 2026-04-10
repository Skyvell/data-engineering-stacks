# Bleeding Edge Data Stack — Monorepo Structure (Revised)

> **Tooling:** uv workspaces · just · ruff · pyright · pre-commit · GitHub Actions

Previous version used `pip`, `Makefile`, flat `pyproject.toml`. Here's the actually modern approach.

---

## What changed and why

| Before (generic) | After (bleeding edge) | Why |
|---|---|---|
| `pip install` | `uv sync` | 10-100× faster, single lockfile, workspace support |
| `pyproject.toml` (flat) | uv workspaces (multi-package) | Each layer is an isolated package with its own deps |
| `Makefile` | `justfile` | Modern syntax, arguments, no tab sensitivity |
| `ruff` (lint only) | `ruff` + `pyright` | Type checking catches bugs before runtime |
| `requirements.txt` | `uv.lock` | Single lockfile across all workspace members |
| `pip install -e ".[dev]"` | `uv sync --all-packages` | One command installs everything |
| Docker for local Postgres | `uv run` + DuckDB local | No Docker needed for local dev |

---

## Repository Structure

```
data-platform/
│
├── pyproject.toml                     # Root workspace definition
├── uv.lock                            # Single lockfile (auto-generated, committed)
├── .python-version                    # 3.13 (managed by uv)
├── justfile                           # Task runner (replaces Makefile)
├── .pre-commit-config.yaml            # Pre-commit hooks: ruff + pyright
├── .env.example                       # Template for secrets
├── README.md
│
├── .github/
│   └── workflows/
│       ├── ci.yml                     # PR: ruff + pyright + tests + sqlmesh plan
│       ├── deploy.yml                 # Merge to main: sqlmesh apply + dagster deploy
│       └── quality.yml                # Scheduled: full soda scan
│
│
│── packages/                          # ── SHARED LIBRARIES ──
│   │
│   └── shared/                        # Shared utilities across all layers
│       ├── pyproject.toml
│       └── src/
│           └── shared/
│               ├── __init__.py
│               ├── config.py          # Pydantic settings: S3, RDS, MotherDuck config
│               ├── connections.py     # DuckDB/MotherDuck connection factory
│               ├── logging.py         # Structured logging (structlog)
│               └── types.py           # Shared type definitions
│
│
├── ingestion/                         # ── LAYER 1: dlt pipelines ──
│   ├── pyproject.toml                 # deps: dlt[motherduck], shared
│   └── src/
│       └── ingestion/
│           ├── __init__.py
│           ├── sources/
│           │   ├── __init__.py
│           │   ├── stripe.py
│           │   ├── hubspot.py
│           │   ├── postgres_cdc.py    # Sling wrapper
│           │   └── google_sheets.py
│           ├── helpers/
│           │   ├── __init__.py
│           │   ├── pagination.py
│           │   └── rate_limiting.py
│           └── schemas/
│               ├── stripe.schema.yaml
│               └── hubspot.schema.yaml
│
│
├── transform/                         # ── LAYER 2: SQLMesh project ──
│   ├── pyproject.toml                 # deps: sqlmesh[duckdb], shared
│   ├── config.yaml                    # SQLMesh config
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_stripe__payments.sql
│   │   │   ├── stg_stripe__customers.sql
│   │   │   ├── stg_hubspot__contacts.sql
│   │   │   └── stg_hubspot__deals.sql
│   │   ├── intermediate/
│   │   │   ├── int_payments_enriched.sql
│   │   │   └── int_contacts_with_deals.sql
│   │   └── marts/
│   │       ├── fct_revenue.sql
│   │       ├── fct_deals.sql
│   │       ├── dim_customers.sql
│   │       └── dim_products.sql
│   ├── audits/
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
│   └── tests/
│       └── test_fct_revenue.yaml
│
│
├── quality/                           # ── LAYER 3: Soda checks ──
│   ├── pyproject.toml                 # deps: soda-core-duckdb, shared
│   ├── soda_config.yml
│   ├── checks/
│   │   ├── raw/
│   │   │   ├── orders.yml
│   │   │   └── customers.yml
│   │   ├── staging/
│   │   │   └── stg_stripe__payments.yml
│   │   └── marts/
│   │       ├── fct_revenue.yml
│   │       └── dim_customers.yml
│   └── src/
│       └── quality/
│           ├── __init__.py
│           └── runner.py              # Programmatic Soda scan runner
│
│
├── orchestration/                     # ── LAYER 4: Dagster ──
│   ├── pyproject.toml                 # deps: dagster, dagster-dlt, dagster-sqlmesh, shared
│   └── src/
│       └── orchestration/
│           ├── __init__.py
│           ├── definitions.py         # Main Dagster entry point
│           ├── assets/
│           │   ├── __init__.py
│           │   ├── ingestion.py       # dlt assets
│           │   ├── transformation.py  # SQLMesh assets
│           │   └── quality.py         # Soda assets
│           ├── resources.py           # DuckDB conn, S3, secrets
│           ├── schedules.py
│           ├── sensors.py
│           └── partitions.py
│
│
├── semantic/                          # ── LAYER 5: Cube.dev (JS — separate from Python workspace) ──
│   ├── package.json                   # Node deps (Cube is JS-based)
│   ├── cube.js                        # Cube config
│   ├── schema/
│   │   ├── Revenue.js
│   │   ├── Customers.js
│   │   ├── Deals.js
│   │   └── Products.js
│   └── Dockerfile
│
│
├── infra/                             # ── INFRASTRUCTURE ──
│   ├── terraform/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   ├── modules/
│   │   │   ├── s3/main.tf            # Data lake bucket
│   │   │   ├── rds/main.tf           # PostgreSQL for DuckLake catalog
│   │   │   ├── ecs/main.tf           # Dagster + Cube containers
│   │   │   ├── iam/main.tf           # Roles and policies
│   │   │   └── networking/main.tf    # VPC, subnets, SGs
│   │   └── environments/
│   │       ├── dev.tfvars
│   │       ├── staging.tfvars
│   │       └── prod.tfvars
│   └── docker/
│       ├── dagster.Dockerfile
│       ├── cube.Dockerfile
│       └── docker-compose.yml         # Local dev: Postgres only (DuckDB runs in-process)
│
│
├── scripts/                           # ── UTILITY SCRIPTS (with inline uv metadata) ──
│   ├── setup_ducklake.py              # /// script | dependencies = ["duckdb"] ///
│   ├── seed_dev_data.py               # /// script | dependencies = ["duckdb", "polars"] ///
│   ├── run_backfill.py
│   ├── export_to_iceberg.py           # Escape hatch: DuckLake → Iceberg
│   └── health_check.py
│
│
└── docs/
    ├── architecture.md
    ├── runbook.md
    ├── onboarding.md
    ├── data-dictionary.md
    └── adr/
        ├── 001-ducklake-over-iceberg.md
        ├── 002-sqlmesh-over-dbt.md
        ├── 003-motherduck-over-athena.md
        ├── 004-dagster-over-airflow.md
        ├── 005-uv-over-poetry.md
        └── 006-just-over-make.md
```

---

## Root pyproject.toml

```toml
[project]
name = "data-platform"
version = "0.1.0"
description = "Bleeding edge data engineering platform"
requires-python = ">=3.13"
# Root has no direct dependencies — each workspace member has its own

[tool.uv.workspace]
members = [
    "packages/*",
    "ingestion",
    "transform",
    "quality",
    "orchestration",
]

[tool.uv]
dev-dependencies = [
    "pytest>=8.0",
    "ruff>=0.8",
    "pyright>=1.1",
    "pre-commit>=4.0",
]

[tool.ruff]
target-version = "py313"
line-length = 120

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "TCH"]

[tool.ruff.format]
quote-style = "double"

[tool.pyright]
pythonVersion = "3.13"
typeCheckingMode = "standard"
```

---

## Workspace Member pyproject.toml Examples

### packages/shared/pyproject.toml
```toml
[project]
name = "shared"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "pydantic-settings>=2.5",
    "structlog>=24.0",
    "duckdb>=1.4",
    "boto3>=1.34",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### ingestion/pyproject.toml
```toml
[project]
name = "ingestion"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "shared",                    # workspace member
    "dlt[motherduck]>=1.0",
]

[tool.uv.sources]
shared = { workspace = true }   # resolved from workspace, not PyPI

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### transform/pyproject.toml
```toml
[project]
name = "transform"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "shared",
    "sqlmesh[duckdb]>=0.90",
]

[tool.uv.sources]
shared = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### orchestration/pyproject.toml
```toml
[project]
name = "orchestration"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "shared",
    "ingestion",                 # depends on ingestion package
    "transform",                 # depends on transform package
    "quality",                   # depends on quality package
    "dagster>=1.7",
    "dagster-webserver>=1.7",
    "dagster-dlt>=0.24",
    "dagster-sqlmesh>=0.2",
]

[tool.uv.sources]
shared = { workspace = true }
ingestion = { workspace = true }
transform = { workspace = true }
quality = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

---

## justfile

```just
# justfile — bleeding edge task runner

set dotenv-load                         # auto-load .env file

# ── Setup ──

# First-time setup: install Python, deps, hooks, DuckLake
setup:
    uv python install 3.13
    uv sync --all-packages
    uv run pre-commit install
    uv run python scripts/setup_ducklake.py
    uv run python scripts/seed_dev_data.py

# ── Development ──

# Start local dev (Dagster UI + local Postgres for DuckLake catalog)
dev:
    docker compose -f infra/docker/docker-compose.yml up -d postgres
    uv run --package orchestration dagster dev

# Start Cube dev server
cube-dev:
    cd semantic && npm run dev

# ── Transforms ──

# Preview SQLMesh changes (column-level diff)
plan env="dev":
    cd transform && uv run sqlmesh plan {{env}}

# Apply SQLMesh changes
apply env="dev":
    cd transform && uv run sqlmesh plan {{env}} --auto-apply

# Diff two environments
diff a="dev" b="prod":
    cd transform && uv run sqlmesh diff {{a}} {{b}}

# Run SQLMesh tests
test-transform:
    cd transform && uv run sqlmesh test

# ── Quality ──

# Run Soda quality checks
quality:
    uv run --package quality soda scan -d motherduck -c quality/soda_config.yml quality/checks/

# ── Testing ──

# Run all tests
test:
    uv run pytest ingestion/ -v
    uv run pytest orchestration/ -v
    just test-transform

# ── Code Quality ──

# Lint and format
lint:
    uv run ruff check .
    uv run ruff format --check .
    uv run pyright

# Fix lint issues
fix:
    uv run ruff check --fix .
    uv run ruff format .

# ── Deployment ──

# Deploy transforms to prod
deploy-transforms:
    cd transform && uv run sqlmesh plan prod --auto-apply

# Deploy Dagster to cloud
deploy-dagster:
    dagster-cloud ci deploy

# Full deploy
deploy: deploy-transforms deploy-dagster

# ── Utilities ──

# Health check all connections
health:
    uv run python scripts/health_check.py

# Export DuckLake to Iceberg (escape hatch)
export-iceberg:
    uv run python scripts/export_to_iceberg.py

# Add a new dlt source (scaffolding)
new-source name:
    touch ingestion/src/ingestion/sources/{{name}}.py
    touch ingestion/schemas/{{name}}.schema.yaml
    @echo "Created source scaffold for {{name}}"
    @echo "Next: edit ingestion/src/ingestion/sources/{{name}}.py"

# Add a new SQLMesh model (scaffolding)
new-model layer name:
    touch transform/models/{{layer}}/{{name}}.sql
    @echo "Created model: transform/models/{{layer}}/{{name}}.sql"

# Lock and update deps
lock:
    uv lock

upgrade:
    uv lock --upgrade
```

---

## .pre-commit-config.yaml

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.8.0
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: local
    hooks:
      - id: pyright
        name: pyright
        entry: uv run pyright
        language: system
        types: [python]
        pass_filenames: false
```

---

## CI/CD (.github/workflows/ci.yml)

```yaml
name: CI

on:
  pull_request:
    branches: [main]

env:
  UV_CACHE_DIR: .uv-cache

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
      - run: uv sync --all-packages
      - run: uv run ruff check .
      - run: uv run ruff format --check .
      - run: uv run pyright

  test:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
      - run: uv sync --all-packages
      - run: uv run pytest ingestion/ -v
      - run: uv run pytest orchestration/ -v

  sqlmesh-plan:
    runs-on: ubuntu-latest
    needs: test
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
      - run: uv sync --package transform
      - run: cd transform && uv run sqlmesh plan --auto-categorize
        env:
          MOTHERDUCK_TOKEN: ${{ secrets.MOTHERDUCK_TOKEN }}

  soda-scan:
    runs-on: ubuntu-latest
    needs: sqlmesh-plan
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
      - run: uv sync --package quality
      - run: uv run --package quality soda scan -d motherduck -c quality/soda_config.yml quality/checks/
        env:
          MOTHERDUCK_TOKEN: ${{ secrets.MOTHERDUCK_TOKEN }}
```

---

## Inline Script Metadata (PEP 723)

Utility scripts don't need to be part of the workspace. They declare their own deps inline:

```python
# scripts/setup_ducklake.py
# /// script
# requires-python = ">=3.13"
# dependencies = ["duckdb>=1.4", "pydantic-settings>=2.5"]
# ///

"""Initialize DuckLake: create catalog schemas and verify connectivity."""

import duckdb

def main():
    conn = duckdb.connect()
    conn.install_extension("ducklake")
    conn.load_extension("ducklake")
    
    conn.sql("""
        ATTACH 'postgres:dbname=ducklake_catalog host=localhost' 
        AS lake (TYPE ducklake, DATA_PATH 's3://my-datalake/warehouse/')
    """)
    
    for schema in ["raw", "staging", "marts"]:
        conn.sql(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
        print(f"✓ Schema lake.{schema} ready")

if __name__ == "__main__":
    main()

# Run with: uv run scripts/setup_ducklake.py
# uv auto-creates a venv with the declared deps — no install step
```

---

## Local Development Flow

```bash
# 1. Clone
git clone git@github.com:yourorg/data-platform.git
cd data-platform

# 2. Setup (uv installs Python 3.13 + all deps in seconds)
just setup

# 3. Develop
just dev                          # Dagster UI at localhost:3000

# 4. Add a new source
just new-source zendesk            # scaffolds files
# edit ingestion/src/ingestion/sources/zendesk.py
# edit orchestration/src/orchestration/assets/ingestion.py

# 5. Add a model
just new-model staging stg_zendesk__tickets
# edit transform/models/staging/stg_zendesk__tickets.sql
just plan                          # see column-level diff
just apply                         # execute in virtual dev env

# 6. Check quality
just quality

# 7. Lint + type check
just lint                          # or `just fix` to auto-fix

# 8. Open PR → CI runs everything
# 9. Merge → CD deploys to prod
```

---

## Environment Strategy

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│    Local Dev      │    │     Staging       │    │    Production     │
│                   │    │                   │    │                   │
│  DuckDB           │    │  MotherDuck       │    │  MotherDuck       │
│  (in-process)     │    │  (staging db)     │    │  (prod db)        │
│                   │    │                   │    │                   │
│  Postgres         │    │  RDS Postgres     │    │  RDS Postgres     │
│  (Docker)         │    │  (staging)        │    │  (prod)           │
│                   │    │                   │    │                   │
│  Local fs or      │    │  S3               │    │  S3               │
│  MinIO            │    │  staging bucket   │    │  prod bucket      │
└──────────────────┘    └──────────────────┘    └──────────────────┘

SQLMesh virtual environments eliminate data duplication.
`just plan staging` previews changes.
`just apply prod` deploys.
```

---

## Why uv Workspaces > Flat pyproject.toml

The key insight: **each layer has different dependencies and they shouldn't pollute each other.**

```
uv sync --package ingestion      # installs only dlt + shared deps
uv sync --package transform      # installs only sqlmesh + shared deps
uv sync --package orchestration  # installs everything (it depends on all layers)
uv sync --all-packages           # installs the full workspace
```

In CI, this means you can **run SQLMesh plan without installing Dagster**, and run Soda scans without installing dlt. Faster CI, smaller containers, clearer dependency boundaries.

The single `uv.lock` at the root guarantees version consistency across all packages — no "works on my machine" because ingestion pinned duckdb 1.3 while transform pinned duckdb 1.4.

---

## Summary: What Makes This Bleeding Edge

| Layer | Tool | Why it's bleeding edge |
|---|---|---|
| Package manager | uv | Replaces pip/poetry/pipenv. 100× faster. Workspace monorepo support. |
| Task runner | just | Replaces Make. Clean syntax, arguments, dotenv loading. |
| Linter + formatter | ruff | Replaces black + isort + flake8 + pyflakes. Single Rust binary. |
| Type checker | pyright | Catches bugs before runtime. Strict mode optional. |
| Pre-commit | ruff + pyright hooks | Every commit is lint-clean and type-safe. |
| Scripts | PEP 723 inline metadata | Utility scripts declare their own deps. `uv run` handles the rest. |
| CI deps | `uv sync --package X` | Install only what each CI job needs. Faster, smaller. |
| Python version | 3.13 (managed by uv) | No pyenv. uv installs Python for you. |
