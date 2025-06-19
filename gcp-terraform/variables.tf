variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The region for project resources"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "The name of the storage bucket"
  type        = string
}
