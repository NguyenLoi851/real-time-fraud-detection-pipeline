# Terraform: Cloud Foundation (Step 4)

## Purpose

Provision minimal GCP foundation resources for lake and warehouse layers.

## Resources

- GCS data lake buckets for Bronze, Silver, and Gold zones
- BigQuery dataset for analytics-ready tables
- `google_storage_bucket.bronze`
- `google_storage_bucket.silver`
- `google_storage_bucket.gold`
- `google_bigquery_dataset.fraud_analytics`

## Prerequisites

Complete shared setup first: [../../docs/prerequisites.md](../../docs/prerequisites.md)

## Create Service Account Key (GCP Console)

1. Open **IAM & Admin** -> **Service Accounts**.
2. Create service account (example `terraform-fraud-infra`).
3. Grant roles:
   - `Storage Admin`
   - `BigQuery Admin`
4. Create JSON key and download it.
5. Store key at `infra/terraform/keys/terraform-sa-key.json`.

## Configure Variables

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and set:

- `project_id`
- `service_account_key_file` (example `./keys/terraform-sa-key.json`)

## Deploy

```bash
terraform init
terraform plan
terraform apply
```

## Outputs

After apply, Terraform prints:

- Bronze/Silver/Gold bucket names
- BigQuery dataset ID and fully-qualified dataset reference

Use these outputs in [../../docs/runbook-gcp.md](../../docs/runbook-gcp.md) and [../../streaming/README.md](../../streaming/README.md).

## Destroy

```bash
terraform destroy
```

If resources are not empty, set `force_destroy = true` in `terraform.tfvars`.

## Troubleshooting

See shared operations guide: [../../docs/operations.md](../../docs/operations.md)
