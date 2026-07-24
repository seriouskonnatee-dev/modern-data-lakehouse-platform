output "data_lake_bucket_name" {
  description = "GCS bucket name for the Bronze/Silver data lake -- used by the Bronze producer and Silver PySpark jobs' --lake-root when running against real GCS."
  value       = google_storage_bucket.data_lake.name
}

output "staging_dataset_id" {
  description = "BigQuery dataset id for dbt staging models."
  value       = google_bigquery_dataset.staging.dataset_id
}

output "marts_dataset_id" {
  description = "BigQuery dataset id for dbt Gold marts -- matches gold/profiles.yml.example's `bigquery` target `dataset`."
  value       = google_bigquery_dataset.marts.dataset_id
}

output "dbt_runner_service_account_email" {
  description = "Service account email for dbt/Airflow to authenticate as against BigQuery + the data lake bucket."
  value       = google_service_account.dbt_runner.email
}
