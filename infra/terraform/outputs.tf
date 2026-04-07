output "bronze_bucket_name" {
  description = "Name of the Bronze bucket."
  value       = google_storage_bucket.bronze.name
}

output "silver_bucket_name" {
  description = "Name of the Silver bucket."
  value       = google_storage_bucket.silver.name
}

output "gold_bucket_name" {
  description = "Name of the Gold bucket."
  value       = google_storage_bucket.gold.name
}

output "platform_bucket_name" {
  description = "Name of the single cloud-native platform bucket."
  value       = google_storage_bucket.platform.name
}

output "fraud_analytics_dataset_id" {
  description = "BigQuery dataset ID."
  value       = google_bigquery_dataset.fraud_analytics.dataset_id
}

output "fraud_analytics_dataset_fqn" {
  description = "BigQuery dataset fully qualified identifier."
  value       = "${var.project_id}.${google_bigquery_dataset.fraud_analytics.dataset_id}"
}