#!/bin/bash
# ─────────────────────────────────────────────────────────────
# Axiom — Google Cloud Run deployment script
# Project:  axiom-gtmvelo
# Service:  axiom-api
# Region:   us-central1
#
# Usage: ./deploy.sh
#
# Always use THIS script — never run gcloud run deploy directly.
# The raw gcloud command skips env vars and Cloud SQL, which
# causes the container to crash at startup (pydantic validation).
# ─────────────────────────────────────────────────────────────
set -euo pipefail

# ── Load .env if it exists (local dev / CI)
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# ── Validate required vars
: "${DATABASE_URL:?DATABASE_URL must be set}"
: "${RUNDOWN_API_KEY:?RUNDOWN_API_KEY must be set}"
: "${AXIOM_INTERNAL_TOKEN:?AXIOM_INTERNAL_TOKEN must be set}"
: "${CLOUD_SQL_INSTANCE:?CLOUD_SQL_INSTANCE must be set}"

PROJECT="${GCLOUD_PROJECT:-axiom-gtmvelo}"
REGION="${GCLOUD_REGION:-us-central1}"
SERVICE="axiom-api"

echo "==> Deploying Axiom to Cloud Run"
echo "    Project : ${PROJECT}"
echo "    Service : ${SERVICE}"
echo "    Region  : ${REGION}"
echo ""

gcloud run deploy "${SERVICE}" \
  --source . \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --platform managed \
  --allow-unauthenticated \
  --port 8080 \
  --memory 1Gi \
  --cpu 1 \
  --min-instances 0 \
  --max-instances 10 \
  --timeout 900 \
  --no-cpu-throttling \
  --add-cloudsql-instances "${CLOUD_SQL_INSTANCE}" \
  --set-env-vars "DATABASE_URL=${DATABASE_URL}" \
  --set-env-vars "RUNDOWN_API_KEY=${RUNDOWN_API_KEY}" \
  --set-env-vars "AXIOM_INTERNAL_TOKEN=${AXIOM_INTERNAL_TOKEN}" \
  --set-env-vars "APP_ENV=production" \
  --set-env-vars "LOG_LEVEL=INFO"

echo ""
echo "✓ Axiom deployed successfully."
echo "  Service URL:"
gcloud run services describe "${SERVICE}" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --format "value(status.url)"
