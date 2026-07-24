# Infrastructure as Code (Terraform)

Documented, reviewable Terraform for the **production** shape of this pipeline on GCP
(free-tier-eligible resource types). This is **not applied** as part of this portfolio
project -- there is no live GCP project or billing account behind it. See the task
instructions this repo was built against: these files are portfolio artifacts showing
IaC competency, not a deployed environment.

## What it provisions

- `storage.tf` -- a GCS bucket for the Bronze/Silver lake (standing in for the local
  `data_lake/` + MinIO setup used to run this project locally), with lifecycle rules to
  transition old Bronze objects to cheaper storage classes.
- `bigquery.tf` -- BigQuery datasets (`staging`, `marts`) matching the schemas this
  repo's dbt project (`gold/`) would target if pointed at the `bigquery` profile
  (see `gold/profiles.yml.example` and `docs/adr/0003-duckdb-vs-bigquery-for-dbt.md`).
- `variables.tf` / `outputs.tf` -- standard parameterization (project id, region,
  environment) and outputs (bucket name, dataset ids) for downstream consumption (e.g.
  by the Airflow deployment or CI).

## Why this isn't applied here

`terraform apply` requires real GCP credentials and an active billing account. Running
it in a portfolio repo's CI, or asking a reviewer to run it, would either fail (no
credentials) or require sharing/creating a throwaway paid cloud account for a portfolio
demo -- neither is a good use of anyone's time. Instead:

- `terraform validate` and `terraform fmt -check` run in CI (`.github/workflows/ci.yml`)
  to prove the configuration is at least syntactically and structurally correct.
- Every resource block is commented explaining what it's for and why it's shaped the
  way it is, so the Terraform is readable as documentation even without being run.

## Running it for real

```bash
cd infra
terraform init
terraform plan -var="gcp_project_id=your-real-project"   # review before applying
terraform apply -var="gcp_project_id=your-real-project"  # requires real GCP credentials
```
