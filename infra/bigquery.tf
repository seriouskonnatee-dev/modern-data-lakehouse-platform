# BigQuery datasets matching the dbt project's schema config (gold/dbt_project.yml:
# staging -> +schema: staging, marts -> +schema: marts) so that pointing gold/'s dbt
# profile at `--target bigquery` (see gold/profiles.yml.example) writes into exactly
# these datasets with no further configuration.

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "staging"
  project     = var.gcp_project_id
  location    = var.region
  description = "dbt staging models -- light typing/renaming over Silver Parquet, no business logic. See gold/models/staging/."
  # No default_table_expiration_ms set: staging models materialize as views (see
  # gold/dbt_project.yml), so there are no accumulating tables here to expire.

  labels = var.labels

  depends_on = [google_project_service.required_apis]
}

resource "google_bigquery_dataset" "marts" {
  dataset_id  = "marts"
  project     = var.gcp_project_id
  location    = var.region
  description = "dbt Gold marts -- dimensional model (dim_*, fct_*) and BI-facing marts (mart_*). See gold/models/marts/ and docs/design.md."

  labels = var.labels

  depends_on = [google_project_service.required_apis]
}

# A dedicated service account for dbt/Airflow to authenticate as when running against
# BigQuery, scoped to just this project rather than reusing a broad default service
# account -- least-privilege, and makes IAM bindings auditable per-pipeline.
resource "google_service_account" "dbt_runner" {
  account_id   = "lakehouse-dbt-runner-${var.environment}"
  display_name = "dbt/Airflow runner for the lakehouse Gold layer"
  project      = var.gcp_project_id
}

resource "google_project_iam_member" "dbt_runner_bigquery_job_user" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser" # can run queries/jobs, not admin the project
  member  = "serviceAccount:${google_service_account.dbt_runner.email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_runner_staging_editor" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  project    = var.gcp_project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dbt_runner.email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_runner_marts_editor" {
  dataset_id = google_bigquery_dataset.marts.dataset_id
  project    = var.gcp_project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dbt_runner.email}"
}

resource "google_storage_bucket_iam_member" "dbt_runner_lake_read" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectViewer" # dbt/BigQuery only needs to READ Silver Parquet
  member = "serviceAccount:${google_service_account.dbt_runner.email}"
}
