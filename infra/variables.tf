variable "gcp_project_id" {
  description = "GCP project id to provision resources in. No default -- must be set explicitly so this can never accidentally target a real project."
  type        = string
}

variable "region" {
  description = "GCP region for regional resources (bucket location, BigQuery dataset location)."
  type        = string
  default     = "asia-southeast1" # Bangkok-adjacent region, matching the retail chain's home market
}

variable "environment" {
  description = "Deployment environment name, used as a resource-naming suffix (dev/staging/prod)."
  type        = string
  default     = "dev"
}

variable "lake_bucket_name" {
  description = "GCS bucket name for the Bronze/Silver data lake. Must be globally unique across all of GCS."
  type        = string
  default     = "lakehouse-retail-data-lake" # override in a real deployment -- bucket names are global
}

variable "bronze_retention_days" {
  description = "Days before Bronze objects transition to Coldline storage (Bronze is rarely re-read once Silver has processed it, but is kept indefinitely for replay -- see docs/adr/0001-medallion-architecture.md)."
  type        = number
  default     = 30
}

variable "labels" {
  description = "Common resource labels applied across all provisioned resources."
  type        = map(string)
  default = {
    project   = "modern-data-lakehouse-platform"
    owner     = "data-engineering"
    portfolio = "true"
  }
}
