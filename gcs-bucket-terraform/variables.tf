variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The region for the bucket"
  type        = string
  default     = "US"
}

variable "bucket_name" {
  description = "The name of the storage bucket"
  type        = string
}
