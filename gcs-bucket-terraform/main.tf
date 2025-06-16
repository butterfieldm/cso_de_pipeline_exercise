provider "google" {
  project = var.project_id
  region  = var.region
}

# Google Cloud Storage Buckets

resource "google_storage_bucket" "bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true  # Optional: allows deletion of non-empty bucket
}

# BigQuery Datasets 

resource "google_bigquery_dataset" "cso_exercise_bq_staging" {
  dataset_id                  = "cso_exercise_bq_staging"
  location                    = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "cso_exercise_bq_curated" {
  dataset_id                  = "cso_exercise_bq_curated"
  location                    = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "cso_exercise_bq_error_hospital" {
  dataset_id                  = "cso_exercise_bq_error_hospital"
  location                    = var.region
  delete_contents_on_destroy = true
}

# BigQuery Tables

# Staging 
resource "google_bigquery_table" "staging_transactions" {
  dataset_id = google_bigquery_dataset.cso_exercise_bq_staging.transactions
  table_id   = "transactions"
  schema     = file("${path.module}/schemas/transactions_schema.json")
  deletion_protection = false
}

resource "google_bigquery_table" "staging_customer" {
  dataset_id = google_bigquery_dataset.cso_exercise_bq_staging.customer
  table_id   = "customer"
  schema     = file("${path.module}/schemas/customer_schema.json")
  deletion_protection = false
}

# Curated 
resource "google_bigquery_table" "curated_transactions" {
  dataset_id = google_bigquery_dataset.cso_exercise_bq_curated.transactions
  table_id   = "transactions"
  schema     = file("${path.module}/schemas/transactions_schema.json")
  deletion_protection = false
}

resource "google_bigquery_table" "curated_customer" {
  dataset_id = google_bigquery_dataset.cso_exercise_bq_curated.customer
  table_id   = "customer"
  schema     = file("${path.module}/schemas/customer_schema.json")
  deletion_protection = false
}

# Error Hospital
resource "google_bigquery_table" "error_transactions" {
  dataset_id = google_bigquery_dataset.error_hospital.transactions
  table_id   = "transactions_errors"
  schema     = file("${path.module}/schemas/errors_schema.json")
  deletion_protection = false
}

resource "google_bigquery_table" "error_customer" {
  dataset_id = google_bigquery_dataset.error_hospital.customer
  table_id   = "customer_errors"
  schema     = file("${path.module}/schemas/errors_schema.json")
  deletion_protection = false
}
