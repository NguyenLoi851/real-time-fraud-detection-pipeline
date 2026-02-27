# Terraform: Cloud Foundation (Step 4)

This Terraform module provisions the minimal GCP foundation for roadmap Step 4:

- GCS data lake buckets for Bronze, Silver, and Gold zones
- BigQuery dataset for analytics-ready tables

## Resources

- `google_storage_bucket.bronze`
- `google_storage_bucket.silver`
- `google_storage_bucket.gold`
- `google_bigquery_dataset.fraud_analytics`

## Prerequisites

- Terraform >= 1.5
- A GCP project with billing enabled
- A Google Cloud service account JSON key file (downloaded locally)

## Create Service Account Key (GCP Console)

1. Open **IAM & Admin** → **Service Accounts** in your GCP project.
2. Click **Create Service Account**.
3. Give it a name such as `terraform-fraud-infra` and continue.
4. Grant these roles (minimum for this module):
	- `Storage Admin`
	- `BigQuery Admin`
5. Create the account.
6. Open the new service account → **Keys** tab.
7. Click **Add Key** → **Create new key** → select **JSON**.
8. Download the JSON key file.

Store the key file in this repo under a local folder such as `infra/terraform/keys/`.

Example:

```bash
cd infra/terraform
mkdir -p keys
# move downloaded key to: infra/terraform/keys/terraform-sa-key.json
```

## Configure Variables

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and set at least:

- `project_id`
- `service_account_key_file` (for example `./keys/terraform-sa-key.json`)

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

## Destroy

```bash
terraform destroy
```

If resources are not empty, set `force_destroy = true` in `terraform.tfvars`.