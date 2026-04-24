#!/bin/bash
# ─────────────────────────────────────────────────────────────
# cloud_scheduler_setup.sh
#
# Sets up Google Cloud Scheduler to run the Axiom daily pipeline
# automatically every morning at 10:00 AM Eastern Time (14:00 UTC).
#
# What it creates:
#   - A Cloud Scheduler job named "axiom-daily-run"
#   - The job POSTs to your Cloud Run service's /v1/tasks/run-daily endpoint
#   - The AXIOM-INTERNAL-TOKEN header is passed securely via a secret
#   - On failure, it retries up to 3 times before alerting
#
# Requirements:
#   - gcloud CLI installed and authenticated (gcloud auth login)
#   - Cloud Run service "axiom-engine" is already deployed
#   - Cloud Scheduler API must be enabled
#
# Run this ONCE to set up the job. Future changes can be made in
# the Google Cloud Console or by re-running this script.
#
# Usage:
#   cd ~/Documents/axiom && bash cloud_scheduler_setup.sh
# ─────────────────────────────────────────────────────────────
set -euo pipefail

PROJECT="axiom-gtmvelo"
REGION="us-central1"
SERVICE="axiom-engine"
JOB_NAME="axiom-daily-run"

# Load environment variables (reads AXIOM_INTERNAL_TOKEN from .env)
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

: "${AXIOM_INTERNAL_TOKEN:?AXIOM_INTERNAL_TOKEN must be set in .env}"

echo ""
echo "==> Setting up Axiom daily auto-run via Google Cloud Scheduler"
echo "    Project : $PROJECT"
echo "    Region  : $REGION"
echo "    Service : $SERVICE"
echo "    Job     : $JOB_NAME"
echo ""

# ─────────────────────────────────────────────────────────────
# Step 1: Enable the Cloud Scheduler API (safe to run even if already enabled)
# ─────────────────────────────────────────────────────────────
echo "==> Enabling Cloud Scheduler API..."
gcloud services enable cloudscheduler.googleapis.com --project="${PROJECT}"

# ─────────────────────────────────────────────────────────────
# Step 2: Get the Cloud Run service URL
# ─────────────────────────────────────────────────────────────
echo "==> Fetching Cloud Run service URL..."
SERVICE_URL=$(gcloud run services describe "${SERVICE}" \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --format="value(status.url)")

if [ -z "${SERVICE_URL}" ]; then
  echo "ERROR: Could not find Cloud Run service '${SERVICE}'."
  echo "       Make sure you've deployed it first: bash deploy.sh"
  exit 1
fi

ENDPOINT="${SERVICE_URL}/v1/tasks/run-daily"
echo "    Endpoint: ${ENDPOINT}"

# ─────────────────────────────────────────────────────────────
# Step 3: Store the token in Google Secret Manager
# (avoids putting the raw token in the scheduler job definition)
# ─────────────────────────────────────────────────────────────
SECRET_NAME="axiom-internal-token"
echo ""
echo "==> Storing AXIOM_INTERNAL_TOKEN in Secret Manager as '${SECRET_NAME}'..."

# Create secret if it doesn't exist
if gcloud secrets describe "${SECRET_NAME}" --project="${PROJECT}" &>/dev/null; then
  echo "    Secret already exists — updating value..."
  echo -n "${AXIOM_INTERNAL_TOKEN}" | \
    gcloud secrets versions add "${SECRET_NAME}" \
      --data-file=- \
      --project="${PROJECT}"
else
  echo "    Creating new secret..."
  echo -n "${AXIOM_INTERNAL_TOKEN}" | \
    gcloud secrets create "${SECRET_NAME}" \
      --data-file=- \
      --project="${PROJECT}"
fi

# ─────────────────────────────────────────────────────────────
# Step 4: Get the Compute Engine default service account
# (Cloud Scheduler uses this to make authenticated calls)
# ─────────────────────────────────────────────────────────────
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT}" \
  --format="value(projectNumber)")
SCHEDULER_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo ""
echo "==> Using service account: ${SCHEDULER_SA}"

# Grant the service account permission to invoke the Cloud Run service
echo "==> Granting Cloud Run invoker permission..."
gcloud run services add-iam-policy-binding "${SERVICE}" \
  --member="serviceAccount:${SCHEDULER_SA}" \
  --role="roles/run.invoker" \
  --project="${PROJECT}" \
  --region="${REGION}"

# Grant permission to read the secret
echo "==> Granting Secret Manager reader permission..."
gcloud secrets add-iam-policy-binding "${SECRET_NAME}" \
  --member="serviceAccount:${SCHEDULER_SA}" \
  --role="roles/secretmanager.secretAccessor" \
  --project="${PROJECT}"

# ─────────────────────────────────────────────────────────────
# Step 5: Create (or update) the Cloud Scheduler job
#
# Schedule: "0 10 * * *" = every day at 10:00 AM Eastern Time (America/New_York)
# The job body passes "target_date": null so the pipeline uses today's date.
# ─────────────────────────────────────────────────────────────
echo ""
echo "==> Creating Cloud Scheduler job '${JOB_NAME}'..."

# Delete existing job if present (so we can re-create cleanly)
if gcloud scheduler jobs describe "${JOB_NAME}" \
     --project="${PROJECT}" \
     --location="${REGION}" &>/dev/null; then
  echo "    Existing job found — deleting for clean re-create..."
  gcloud scheduler jobs delete "${JOB_NAME}" \
    --project="${PROJECT}" \
    --location="${REGION}" \
    --quiet
fi

gcloud scheduler jobs create http "${JOB_NAME}" \
  --project="${PROJECT}" \
  --location="${REGION}" \
  --schedule="0 10 * * *" \
  --time-zone="America/New_York" \
  --uri="${ENDPOINT}" \
  --http-method=POST \
  --message-body='{"target_date": null, "dry_run": false}' \
  --headers="Content-Type=application/json,AXIOM-INTERNAL-TOKEN=${AXIOM_INTERNAL_TOKEN}" \
  --oidc-service-account-email="${SCHEDULER_SA}" \
  --oidc-token-audience="${SERVICE_URL}" \
  --attempt-deadline=300s \
  --max-retry-attempts=3 \
  --max-backoff=3600s \
  --min-backoff=300s \
  --description="Axiom daily scoring pipeline — runs every morning at 10am ET"

echo ""
echo "════════════════════════════════════════════════════════"
echo "  ✓ Cloud Scheduler job created successfully!"
echo ""
echo "  Schedule   : Every day at 10:00 AM Eastern Time"
echo "  Endpoint   : ${ENDPOINT}"
echo "  Retries    : 3 (with 5-minute initial backoff)"
echo "  Timeout    : 5 minutes per attempt"
echo ""
echo "  To trigger it manually RIGHT NOW (for testing):"
echo "    gcloud scheduler jobs run ${JOB_NAME} \\"
echo "      --project=${PROJECT} --location=${REGION}"
echo ""
echo "  To view job history and logs:"
echo "    gcloud scheduler jobs describe ${JOB_NAME} \\"
echo "      --project=${PROJECT} --location=${REGION}"
echo ""
echo "  Or open the console:"
echo "    https://console.cloud.google.com/cloudscheduler?project=${PROJECT}"
echo "════════════════════════════════════════════════════════"
echo ""
