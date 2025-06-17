provider "google" {
  project = var.project_id
  region  = var.region
}

## Google Cloud Storage Buckets
resource "google_storage_bucket" "bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
}

## BigQuery Datasets 
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

## BigQuery Tables

# Staging 
resource "google_bigquery_table" "staging_transactions" {
  dataset_id         = google_bigquery_dataset.cso_exercise_bq_staging.dataset_id
  table_id           = "transactions"
  schema             = file("${path.module}/../schemas/transactions_schema.json")
  deletion_protection = false
}

resource "google_bigquery_table" "staging_customer" {
  dataset_id         = google_bigquery_dataset.cso_exercise_bq_staging.dataset_id
  table_id           = "customer"
  schema             = file("${path.module}/../schemas/customer_schema.json")
  deletion_protection = false
}

# Curated 
resource "google_bigquery_table" "curated_transactions" {
  dataset_id         = google_bigquery_dataset.cso_exercise_bq_curated.dataset_id
  table_id           = "transactions"
  schema             = file("${path.module}/../schemas/transactions_schema.json")
  deletion_protection = false
}

resource "google_bigquery_table" "curated_customer" {
  dataset_id         = google_bigquery_dataset.cso_exercise_bq_curated.dataset_id
  table_id           = "customer"
  schema             = file("${path.module}/../schemas/customer_schema.json")
  deletion_protection = false
}

# Error Hospital
resource "google_bigquery_table" "error_transactions" {
  dataset_id         = google_bigquery_dataset.cso_exercise_bq_error_hospital.dataset_id
  table_id           = "transactions_error_hospital"
  schema             = file("${path.module}/../schemas/error_hospital_schema.json")
  deletion_protection = false
}

resource "google_bigquery_table" "error_customer" {
  dataset_id         = google_bigquery_dataset.cso_exercise_bq_error_hospital.dataset_id
  table_id           = "customer_error_hospital"
  schema             = file("${path.module}/../schemas/error_hospital_schema.json")
  deletion_protection = false
}

## PubSub Notification
resource "google_pubsub_topic" "gcs_notifications" {
  name = "gcs-new-file-topic"
}

resource "google_storage_notification" "notify_uploads" {
  bucket         = google_storage_bucket.bucket.name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.gcs_notifications.id
  event_types    = ["OBJECT_FINALIZE"]
}

resource "google_pubsub_topic" "gcs_upload_topic" {
  name = "gcs-file-drop-topic"
}

resource "google_pubsub_subscription" "gcs_upload_sub" {
  name  = "gcs-upload-sub"
  topic = google_pubsub_topic.gcs_upload_topic.name
}

resource "google_project_service" "pubsub" {
  project = var.project_id
  service = "pubsub.googleapis.com"
}

# Get the project number dynamically
data "google_project" "project" {
  project_id = var.project_id
}

# Allow GCS's internal service account to publish to Pub/Sub
resource "google_pubsub_topic_iam_member" "allow_gcs_publish" {
  topic  = google_pubsub_topic.gcs_notifications.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "gcs_publish_permission" {
  topic    = google_pubsub_topic.gcs_notifications.name
  role     = "roles/pubsub.publisher"
  member   = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
}
