# GCS bucket standing in for the local data_lake/ + MinIO setup used to run this
# pipeline locally (see docs/adr/0002-mock-streaming-vs-real-kafka.md). Bronze and
# Silver both live in this bucket under separate prefixes -- one bucket, not two, since
# they share the same lifecycle/retention requirements and access patterns (Spark jobs
# reading/writing Parquet), unlike Gold which needs a queryable warehouse (BigQuery).

resource "google_storage_bucket" "data_lake" {
  name     = "${var.lake_bucket_name}-${var.environment}"
  location = var.region
  project  = var.gcp_project_id

  # Free-tier-friendly: standard storage class for hot (Silver-adjacent) access, with a
  # lifecycle rule (below) moving cold Bronze partitions to Coldline automatically
  # rather than provisioning a second bucket.
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true # simpler, more auditable IAM than per-object ACLs

  # Bronze is append-only and effectively immutable once written (see ADR 0001) -- this
  # is a defense-in-depth guard against a buggy job accidentally overwriting raw history.
  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age            = var.bronze_retention_days
      matches_prefix = ["bronze/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  # Old object versions (from the versioning guard above) don't need to live forever --
  # clean them up after 90 days to control cost.
  lifecycle_rule {
    condition {
      age        = 90
      with_state = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  labels = var.labels

  depends_on = [google_project_service.required_apis]
}

# Explicit, empty placeholder objects for the top-level prefixes -- purely cosmetic
# (GCS has no real "folders") but makes the bucket layout self-documenting in the
# console for anyone browsing it, mirroring the local data_lake/{bronze,silver}/
# structure used when running this pipeline locally.
resource "google_storage_bucket_object" "bronze_prefix_marker" {
  name    = "bronze/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "placeholder -- see docs/design.md for the Bronze contract"
}

resource "google_storage_bucket_object" "silver_prefix_marker" {
  name    = "silver/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "placeholder -- see docs/design.md for the Silver contract"
}
