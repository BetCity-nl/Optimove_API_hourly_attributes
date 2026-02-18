# Optimove API Hourly Attributes

This repository syncs hourly user attribute differences to Optimove.

## What Gets Deployed

This project is deployed as a **Cloud Run Job** (best fit for batch scripts like `main.py`).
No Dockerfile is required.

On every push to the `main` branch, GitHub Actions:

1. Authenticates to Google Cloud using Workload Identity Federation.
2. Builds from source using Cloud Buildpacks.
3. Deploys/updates a Cloud Run Job.

## Required Environment Variables

`main.py` expects these runtime variables:

- `API_URL_ADD_ATTRIBUTES`
- `PROJECT`
- `OPTIMOVE_DATASET`
- `OPTI_TABLE_OPTIMOVE`
- `OPTI_TABLE_DIFFERENCE`
- `USERS_DATASET`
- `USERS_TABLE`

`helpers.py` also reads Secret Manager secret:

- `projects/24200348636/secrets/optimove_api_key` (version `latest`)

## 1) One-Time Google Cloud Setup

Replace placeholders before running:

- `YOUR_PROJECT_ID`
- `YOUR_PROJECT_NUMBER`
- `YOUR_REGION` (example: `europe-west1`)
- `YOUR_REPO` (GitHub repo name)
- `YOUR_GITHUB_USER` (GitHub org/user)

```bash
gcloud config set project YOUR_PROJECT_ID

# Service account used by Cloud Run Job at runtime
gcloud iam service-accounts create optimove-hourly-runner \
  --display-name="Optimove Hourly Cloud Run Job Runner"

# Minimum roles for runtime behavior in this codebase
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:optimove-hourly-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:optimove-hourly-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:optimove-hourly-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Service account used by GitHub Actions to build and deploy
gcloud iam service-accounts create github-deployer \
  --display-name="GitHub Actions Cloud Run Deployer"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:github-deployer@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:github-deployer@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/cloudbuild.builds.editor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:github-deployer@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:github-deployer@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

# Enable required APIs
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  iamcredentials.googleapis.com \
  sts.googleapis.com \
  secretmanager.googleapis.com
```

## 2) Configure Workload Identity Federation (GitHub -> GCP)

```bash
# Create pool
gcloud iam workload-identity-pools create github-pool \
  --project=YOUR_PROJECT_ID \
  --location=global \
  --display-name="GitHub Pool"

# Create provider
gcloud iam workload-identity-pools providers create-oidc github-provider \
  --project=YOUR_PROJECT_ID \
  --location=global \
  --workload-identity-pool=github-pool \
  --display-name="GitHub Provider" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository"

# Allow this specific repo to impersonate deployer SA
gcloud iam service-accounts add-iam-policy-binding \
  github-deployer@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --project=YOUR_PROJECT_ID \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/YOUR_PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/attribute.repository/YOUR_GITHUB_USER/YOUR_REPO"
```

Get the provider resource name (needed in GitHub secret):

```bash
gcloud iam workload-identity-pools providers describe github-provider \
  --project=YOUR_PROJECT_ID \
  --location=global \
  --workload-identity-pool=github-pool \
  --format="value(name)"
```

## 3) Set GitHub Secrets and Variables

In GitHub repo settings:

### Secrets

- `GCP_WORKLOAD_IDENTITY_PROVIDER`  
  Example: `projects/123456789/locations/global/workloadIdentityPools/github-pool/providers/github-provider`
- `GCP_SERVICE_ACCOUNT`  
  Example: `github-deployer@YOUR_PROJECT_ID.iam.gserviceaccount.com`
- `API_URL_ADD_ATTRIBUTES`
- `PROJECT`
- `OPTIMOVE_DATASET`
- `OPTI_TABLE_OPTIMOVE`
- `OPTI_TABLE_DIFFERENCE`
- `USERS_DATASET`
- `USERS_TABLE`

### Variables

- `GCP_PROJECT_ID`
- `GCP_REGION` (example: `europe-west1`)
- `CLOUD_RUN_JOB_NAME` (example: `optimove-hourly-attributes`)
- `RUNTIME_SERVICE_ACCOUNT` (example: `optimove-hourly-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com`)

## 4) CI/CD Trigger

Workflow file: `.github/workflows/deploy-cloud-run-job.yml`

This workflow runs automatically on:

- Push to `main`
- Manual trigger (`workflow_dispatch`)

## 5) Manual Run After Deploy

Deploying updates the job definition. To execute the job immediately:

```bash
gcloud run jobs execute YOUR_JOB_NAME --region YOUR_REGION --project YOUR_PROJECT_ID
```

## Notes

- If you need automatic execution every hour, add **Cloud Scheduler** to call `gcloud run jobs execute` (or invoke via authenticated HTTP through a small runner service/workflow).
- Keep `.env` local only; production values should remain in GitHub Secrets / Secret Manager.
