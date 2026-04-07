locals {
  base_bucket_name = lower("${var.gcs_bucket_prefix}-${var.environment}-${var.project_id}")
}

resource "google_storage_bucket" "bronze" {
  name                        = "${local.base_bucket_name}-bronze"
  location                    = var.bucket_location
  force_destroy               = var.force_destroy
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  labels = merge(var.labels, {
    environment = var.environment
    zone        = "bronze"
  })
}

resource "google_storage_bucket" "silver" {
  name                        = "${local.base_bucket_name}-silver"
  location                    = var.bucket_location
  force_destroy               = var.force_destroy
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  labels = merge(var.labels, {
    environment = var.environment
    zone        = "silver"
  })
}

resource "google_storage_bucket" "gold" {
  name                        = "${local.base_bucket_name}-gold"
  location                    = var.bucket_location
  force_destroy               = var.force_destroy
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  labels = merge(var.labels, {
    environment = var.environment
    zone        = "gold"
  })
}

resource "google_storage_bucket" "platform" {
  name                        = "${local.base_bucket_name}-${var.platform_bucket_suffix}"
  location                    = var.bucket_location
  force_destroy               = var.force_destroy
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  labels = merge(var.labels, {
    environment = var.environment
    zone        = "platform"
  })
}

resource "google_bigquery_dataset" "fraud_analytics" {
  dataset_id                 = var.bq_dataset_id
  location                   = var.bq_location
  description                = "Fraud analytics curated dataset"
  delete_contents_on_destroy = var.force_destroy

  labels = merge(var.labels, {
    environment = var.environment
  })
}