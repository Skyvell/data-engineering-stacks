# Modern Terraform project structure and deployment guide

A complete reference for structuring Terraform code and deploying it with GitHub Actions. This document captures the cleanest, most modern approach for deploying an application to multiple AWS environments (dev, int, prod) with zero duplication, isolated state, and a pipeline that handles all applies.

---

## Core principles

**1. The environment is data, not code.** There is one set of Terraform files. Environments differ only through variable values and backend configuration. No duplicated `main.tf` files, no per-environment folders containing Terraform code.

**2. One module, many files.** Terraform merges all `.tf` files in a directory automatically. A single `modules/app/` directory contains all infrastructure, split by concern into separate files (networking, compute, database, etc.). This is one module with many files, not many modules.

**3. Locally you plan, the pipeline applies.** Developers run `terraform plan` locally to check their work before pushing. All `terraform apply` operations go through the CI/CD pipeline, which adds linting, security scanning, and approval gates. There are no local apply scripts.

**4. Separate state per environment.** Each environment gets its own S3 backend key and DynamoDB lock table entry. A mistake in dev can never corrupt prod state.

**5. No unnecessary tooling.** No Terragrunt, no Makefile, no task runners. The project uses raw Terraform commands and a few tiny shell scripts for local convenience.

---

## Project structure

```
project/
├── src/                            # Application code
│   └── ...
├── infrastructure/
│   ├── modules/
│   │   └── app/
│   │       ├── variables.tf        # All input variables
│   │       ├── outputs.tf          # All outputs
│   │       ├── locals.tf           # Derived values and naming
│   │       ├── networking.tf       # VPC, subnets, security groups
│   │       ├── compute.tf          # ECS/EC2/Lambda, ASG, ALB
│   │       ├── database.tf         # RDS, DynamoDB, ElastiCache
│   │       ├── iam.tf              # Roles and policies
│   │       └── monitoring.tf       # CloudWatch, alarms
│   ├── live/
│   │   ├── main.tf                 # Single module call
│   │   ├── variables.tf            # Variable declarations (pass-through)
│   │   ├── providers.tf            # AWS provider with assume_role
│   │   └── backend.tf              # Empty S3 backend (filled at init)
│   ├── config/
│   │   ├── dev.tfvars              # Dev variable values
│   │   ├── int.tfvars              # Int variable values
│   │   ├── prod.tfvars             # Prod variable values
│   │   ├── dev.s3.tfbackend        # Dev state backend config
│   │   ├── int.s3.tfbackend        # Int state backend config
│   │   └── prod.s3.tfbackend       # Prod state backend config
│   ├── plan-dev.sh                 # Local plan script for dev
│   ├── plan-int.sh                 # Local plan script for int
│   ├── plan-prod.sh                # Local plan script for prod
│   ├── .tflint.hcl                 # Lint configuration
│   └── .trivyignore                # Security scan exceptions
├── .github/
│   └── workflows/
│       ├── ci.yml                  # PR checks (fmt, validate, lint, scan, plan)
│       ├── deploy.yml              # Deploy to any environment
│       └── drift.yml               # Scheduled drift detection
├── .gitignore
└── README.md
```

### Why this structure

**`modules/app/`** contains all infrastructure logic in one place. Splitting by concern (networking, compute, database) keeps files focused and readable without introducing the complexity of multiple independent modules with separate state. This is the right starting point for a single-app deployment. Split into multiple modules only when you need independent deploy cycles (e.g. shipping a database migration without touching compute).

**`live/`** is the single root configuration. This is the only directory where Terraform commands run. It contains a thin wrapper that calls the module and passes through variables. There is one `main.tf`, one `providers.tf`, one `backend.tf`. Adding a new environment means adding two files to `config/`, nothing in `live/`.

**`config/`** is pure data. No Terraform code. Two files per environment: a `.tfvars` file with variable values and a `.s3.tfbackend` file with state backend configuration. This is where all environment differences live.

**`plan-*.sh`** scripts are three-line convenience wrappers for local use. They init and plan only. There are no apply scripts because applying is the pipeline's job.

---

## File contents

### infrastructure/live/backend.tf

The backend block is intentionally empty. It gets filled at init time via the `-backend-config` flag, which points to the appropriate `.s3.tfbackend` file.

```hcl
terraform {
  backend "s3" {}
}
```

### infrastructure/config/dev.s3.tfbackend

Each environment's backend file specifies where its state lives. The `key` field is the only value that differs between environments.

```hcl
bucket         = "myapp-terraform-state"
key            = "dev/terraform.tfstate"
region         = "eu-north-1"
dynamodb_table = "terraform-locks"
encrypt        = true
```

### infrastructure/config/int.s3.tfbackend

```hcl
bucket         = "myapp-terraform-state"
key            = "int/terraform.tfstate"
region         = "eu-north-1"
dynamodb_table = "terraform-locks"
encrypt        = true
```

### infrastructure/config/prod.s3.tfbackend

```hcl
bucket         = "myapp-terraform-state"
key            = "prod/terraform.tfstate"
region         = "eu-north-1"
dynamodb_table = "terraform-locks"
encrypt        = true
```

### infrastructure/config/dev.tfvars

Variable values for dev. Small instances, minimal redundancy, cost-optimized.

```hcl
environment   = "dev"
instance_type = "t3.small"
db_class      = "db.t3.medium"
vpc_cidr      = "10.0.0.0/16"
desired_count = 1

features = {
  waf_enabled         = false
  nat_gateway         = false
  multi_az_rds        = false
  enhanced_monitoring = false
  cloudfront          = false
}
```

### infrastructure/config/int.tfvars

Integration mirrors prod sizing where it matters for realistic testing, but skips expensive extras.

```hcl
environment   = "int"
instance_type = "t3.medium"
db_class      = "db.t3.large"
vpc_cidr      = "10.1.0.0/16"
desired_count = 2

features = {
  waf_enabled         = true
  nat_gateway         = true
  multi_az_rds        = false
  enhanced_monitoring = false
  cloudfront          = false
}
```

### infrastructure/config/prod.tfvars

Production. Full redundancy, full security, full monitoring.

```hcl
environment   = "prod"
instance_type = "m6i.xlarge"
db_class      = "db.r6g.large"
vpc_cidr      = "10.2.0.0/16"
desired_count = 3

features = {
  waf_enabled         = true
  nat_gateway         = true
  multi_az_rds        = true
  enhanced_monitoring = true
  cloudfront          = true
}
```

### infrastructure/live/main.tf

A single module call. This is the only place infrastructure is invoked.

```hcl
module "app" {
  source = "../modules/app"

  environment   = var.environment
  instance_type = var.instance_type
  db_class      = var.db_class
  vpc_cidr      = var.vpc_cidr
  desired_count = var.desired_count
  features      = var.features
}
```

### infrastructure/live/variables.tf

Declares the same variables the module expects. These are passed through from the `.tfvars` files.

```hcl
variable "environment" {
  type        = string
  description = "Environment name (dev, int, prod)"
}

variable "instance_type" {
  type = string
}

variable "db_class" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "desired_count" {
  type = number
}

variable "features" {
  type = object({
    waf_enabled         = bool
    nat_gateway         = bool
    multi_az_rds        = bool
    enhanced_monitoring = bool
    cloudfront          = bool
  })
}

variable "aws_account_id" {
  type        = string
  description = "AWS account ID for the target environment"
}
```

### infrastructure/live/providers.tf

Uses `assume_role` for cross-account deploys. Each environment's AWS account ID comes from GitHub Environment variables in CI, or from the `.tfvars` file locally.

```hcl
terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-north-1"

  assume_role {
    role_arn = "arn:aws:iam::${var.aws_account_id}:role/TerraformDeployRole"
  }

  default_tags {
    tags = {
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
```

### infrastructure/modules/app/variables.tf

All input variables the module accepts. These define every knob an environment can turn.

```hcl
variable "environment" {
  type        = string
  description = "Environment name"
}

variable "instance_type" {
  type = string
}

variable "db_class" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "desired_count" {
  type = number
}

variable "features" {
  type = object({
    waf_enabled         = bool
    nat_gateway         = bool
    multi_az_rds        = bool
    enhanced_monitoring = bool
    cloudfront          = bool
  })
}
```

### infrastructure/modules/app/locals.tf

Derived values used across all resource files. Naming conventions, conditional logic, and shared tags live here so individual resource files stay clean.

```hcl
locals {
  name_prefix = "myapp-${var.environment}"
  is_prod     = var.environment == "prod"

  az_count = local.is_prod ? 3 : 2

  common_tags = {
    Project     = "myapp"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
```

### infrastructure/modules/app/networking.tf (example)

Resources reference variables and locals. Environment-specific resources use the `features` object for clean conditionals instead of scattering `local.is_prod` checks everywhere.

```hcl
resource "aws_vpc" "main" {
  cidr_block = var.vpc_cidr

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-vpc"
  })
}

resource "aws_nat_gateway" "main" {
  count = var.features.nat_gateway ? local.az_count : 0
  # ...
}
```

### infrastructure/modules/app/outputs.tf

```hcl
output "vpc_id" {
  value = aws_vpc.main.id
}

# Add other outputs as needed
```

---

## Local plan scripts

Each script is three lines. They init and plan only. No apply.

### infrastructure/plan-dev.sh

```bash
#!/usr/bin/env bash
set -euo pipefail
terraform -chdir=live init -backend-config=../config/dev.s3.tfbackend -reconfigure
terraform -chdir=live plan -var-file=../config/dev.tfvars
```

### infrastructure/plan-int.sh

```bash
#!/usr/bin/env bash
set -euo pipefail
terraform -chdir=live init -backend-config=../config/int.s3.tfbackend -reconfigure
terraform -chdir=live plan -var-file=../config/int.tfvars
```

### infrastructure/plan-prod.sh

```bash
#!/usr/bin/env bash
set -euo pipefail
terraform -chdir=live init -backend-config=../config/prod.s3.tfbackend -reconfigure
terraform -chdir=live plan -var-file=../config/prod.tfvars
```

Run from inside the `infrastructure/` directory:

```bash
cd infrastructure
./plan-dev.sh
```

---

## CI/CD pipeline

### How it works

- **PR opened** — runs format check, validation, linting, security scan, and a plan against dev. Posts the plan output as a PR comment so reviewers see exactly what will change.
- **PR merged to main** — auto-deploys to dev.
- **Manual trigger** — pick int or prod from a dropdown in the GitHub Actions UI. Prod requires approval from designated reviewers via GitHub Environments.
- **Scheduled drift detection** — runs weekday mornings against all environments. Alerts if someone made manual changes in the AWS console.

### GitHub Environment setup

In the repository settings under Environments, create three environments:

- `dev` — no protection rules, stores `AWS_ROLE_ARN` variable pointing to the dev AWS account
- `int` — optional reviewers, stores `AWS_ROLE_ARN` for the int account
- `prod` — required reviewers enabled with team leads as approvers, stores `AWS_ROLE_ARN` for the prod account

Each environment's `AWS_ROLE_ARN` variable contains the ARN of an IAM role configured for GitHub OIDC federation.

### .github/workflows/ci.yml

Runs on every pull request that touches infrastructure files. Gates merging on code quality and security.

```yaml
name: Terraform CI

on:
  pull_request:
    paths: ["infrastructure/**"]

permissions:
  id-token: write
  contents: read
  pull-requests: write

jobs:
  quality:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.9"

      - name: Format check
        run: terraform fmt -check -recursive
        working-directory: infrastructure

      - name: Validate
        run: |
          terraform -chdir=live init -backend=false
          terraform -chdir=live validate
        working-directory: infrastructure

      - name: Lint
        uses: terraform-linters/setup-tflint@v4
      - run: |
          tflint --chdir=infrastructure/live --init
          tflint --chdir=infrastructure/live

      - name: Security scan
        uses: aquasecurity/trivy-action@master
        with:
          scan-type: config
          scan-ref: infrastructure
          exit-code: 1
          severity: HIGH,CRITICAL

  plan:
    needs: quality
    runs-on: ubuntu-latest
    environment: dev
    steps:
      - uses: actions/checkout@v4

      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.9"

      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_ROLE_ARN }}
          aws-region: eu-north-1

      - name: Plan
        id: plan
        run: |
          terraform -chdir=infrastructure/live init -backend-config=../config/dev.s3.tfbackend
          terraform -chdir=infrastructure/live plan -var-file=../config/dev.tfvars -no-color 2>&1 | tee plan.txt

      - name: Comment plan on PR
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            let plan = fs.readFileSync('plan.txt', 'utf8');
            if (plan.length > 60000) plan = plan.substring(0, 60000) + '\n... truncated';
            await github.rest.issues.createComment({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: context.issue.number,
              body: `### Terraform plan (dev)\n\`\`\`\n${plan}\n\`\`\``
            });
```

### .github/workflows/deploy.yml

Handles all deployments. Auto-deploys to dev on merge. Manual trigger for int and prod.

```yaml
name: Deploy

on:
  push:
    branches: [main]
    paths: ["infrastructure/**"]
  workflow_dispatch:
    inputs:
      environment:
        type: choice
        options: [dev, int, prod]

permissions:
  id-token: write
  contents: read

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: ${{ inputs.environment || 'dev' }}
    env:
      ENV: ${{ inputs.environment || 'dev' }}

    steps:
      - uses: actions/checkout@v4

      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.9"

      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_ROLE_ARN }}
          aws-region: eu-north-1

      - name: Init
        run: terraform -chdir=infrastructure/live init -backend-config=../config/${ENV}.s3.tfbackend

      - name: Plan
        run: terraform -chdir=infrastructure/live plan -var-file=../config/${ENV}.tfvars -out=tfplan

      - name: Apply
        run: terraform -chdir=infrastructure/live apply tfplan
```

### .github/workflows/drift.yml

Runs on a schedule to detect manual changes made outside Terraform.

```yaml
name: Drift detection

on:
  schedule:
    - cron: "0 8 * * 1-5"

permissions:
  id-token: write
  contents: read

jobs:
  detect:
    strategy:
      matrix:
        env: [dev, int, prod]
    runs-on: ubuntu-latest
    environment: ${{ matrix.env }}

    steps:
      - uses: actions/checkout@v4

      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.9"

      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_ROLE_ARN }}
          aws-region: eu-north-1

      - name: Detect drift
        id: drift
        run: |
          terraform -chdir=infrastructure/live init -backend-config=../config/${{ matrix.env }}.s3.tfbackend
          terraform -chdir=infrastructure/live plan -var-file=../config/${{ matrix.env }}.tfvars -detailed-exitcode
        continue-on-error: true

      - name: Alert on drift
        if: steps.drift.outputs.exitcode == '2'
        run: echo "::warning::Drift detected in ${{ matrix.env }}!"
        # Connect this to Slack, PagerDuty, or your alerting tool
```

---

## Deployment flow

| Trigger | Environment | Approval | What happens |
|---|---|---|---|
| PR opened | — | — | Format, validate, lint, security scan, plan against dev, comment on PR |
| PR merged to main | dev | None | Auto-deploy to dev |
| Manual dispatch | int | Optional | Deploy to int |
| Manual dispatch | prod | Required | Reviewer approves, then deploy to prod |
| Cron (weekday 8am) | All | — | Drift detection, alert if changes found |

---

## Key decisions and reasoning

### Why one module instead of many?

For a single-app deployment, one module avoids the overhead of managing separate state files, cross-module references, and dependency ordering. All resources are tightly coupled anyway (security groups reference ALBs, ALBs reference ECS services). Split into multiple modules only when you need independent deploy cycles or when `terraform plan` gets unacceptably slow (roughly above 50 resources).

### Why flat structure instead of per-environment folders?

Per-environment folders (environments/dev/, environments/int/, environments/prod/) duplicate `main.tf`, `providers.tf`, and `backend.tf` across each folder. They drift apart over time. The flat structure has zero duplication. Adding a new environment means adding two config files. No Terraform code changes.

### Why not Terraform workspaces?

Workspaces share the same backend configuration and code path. It's too easy to run `terraform workspace select prod` and apply the wrong changes. Separate backend configs per environment are explicit and safe.

### Why not Terragrunt?

For a single-module app, Terragrunt adds dependency management and a DRY syntax layer you don't need. It becomes valuable when you have multiple independent modules with cross-references (database needs VPC ID from networking module). Start without it. Add it when you split modules.

### Why not a Makefile or task runner?

The local workflow is two commands: init and plan. Shell history handles the repetition. A tiny plan script per environment is simpler than learning Make syntax, Justfile DSL, or Taskfile YAML. The pipeline runs raw Terraform commands and should never depend on a task runner.

### Why plan-only scripts with no local apply?

If developers can apply locally, they bypass linting, security scanning, and the audit trail. All applies should go through the pipeline where they're gated by quality checks and approvals. Local workflow is for previewing changes before pushing.

### Why a features object instead of is_prod checks?

`count = local.is_prod ? 1 : 0` scattered across files is hard to read and couples resource creation to environment names. A `features` object in the tfvars file makes each environment's capabilities explicit and independent. Int can have WAF enabled but not CloudFront without touching any Terraform code.

---

## When to evolve this structure

**Split into multiple modules** when you need independent deploy cycles or plan times exceed 30 seconds. Use `moved` blocks to refactor state without manual state surgery.

**Add Terragrunt** when you have three or more independent modules with dependency relationships between them.

**Consider OpenTofu** as a drop-in replacement for Terraform if the BSL license is a concern. Same HCL, same providers, same structure.

**Add a `global/` directory** for resources that exist once across all environments (shared IAM roles, DNS hosted zones, the state bucket itself).
