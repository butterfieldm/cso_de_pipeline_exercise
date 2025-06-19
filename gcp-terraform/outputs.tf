output "bucket_url" {
  value = "gs://${google_storage_bucket.bucket.name}/"
}
