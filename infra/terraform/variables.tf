variable "project_id" {
  type        = string
  description = "GCP project ID where resources are created."
}

variable "service_account_key_file" {
  type        = string
  description = "Path to the GCP service account JSON key file used by Terraform."
}

variable "region" {
  type        = string
  description = "Default GCP region for regional resources."
  default     = "us-central1"
}

variable "bucket_location" {
  type        = string
  description = "Location for GCS buckets (for example, US-CENTRAL1 or US)."
  default     = "US-CENTRAL1"
}

variable "bq_location" {
  type        = string
  description = "Location for BigQuery datasets (for example, US or EU)."
  default     = "US"
}

variable "environment" {
  type        = string
  description = "Environment suffix used in resource names."
  default     = "dev"
}

variable "gcs_bucket_prefix" {
  type        = string
  description = "Prefix for datalake bucket names."
  default     = "fraud-datalake"
}

variable "bq_dataset_id" {
  type        = string
  description = "BigQuery dataset ID for analytics tables."
  default     = "fraud_analytics"
}

variable "force_destroy" {
  type        = bool
  description = "Allow destroying non-empty buckets and datasets. Use false for safety."
  default     = false
}

variable "labels" {
  type        = map(string)
  description = "Common labels applied to all resources."
  default = {
    app        = "real-time-fraud-detection"
    managed_by = "terraform"
  }
}