terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Deliberately no `backend` block configured here -- a real deployment would use a GCS
# backend for remote state (`terraform { backend "gcs" { ... } }`), but that requires a
# pre-existing bucket + credentials this portfolio repo doesn't have. Documented rather
# than guessed at: see docs/adr referenced from infra/README.md.
