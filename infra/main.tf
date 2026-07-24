provider "google" {
  project = var.gcp_project_id
  region  = var.region
}

# Enables the GCP APIs this stack actually uses. Listed explicitly (rather than assumed
# pre-enabled) so `terraform apply` on a brand-new project works without a manual
# `gcloud services enable` step first.
resource "google_project_service" "required_apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
  ])

  project            = var.gcp_project_id
  service            = each.value
  disable_on_destroy = false # don't disable shared project APIs just because this stack is torn down
}
