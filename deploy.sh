#!/bin/bash
# ─────────────────────────────────────────────────────────────
# Axiom — Google Cloud Run deployment script
# Project:  axiom-gtmvelo
# Service:  axiom-engine
# Region:   us-central1
# ─────────────────────────────────────────────────────────────
set -euo pipefail

# ── Required environment variables — must be set before running this script
# Load from .env if it exists, otherwise these must be set in your shell
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Validate required vars
: "${DATABASE_URL:?DATABASE_URL must be set}"
: "${RUNDOWN_API_KEY:?RUNDOWN_API_KEY must be set}"
: "${AXIOM_INTERNAL_TOKEN:?AXIOM_INTERNAL_TOKEN must be set}"
: "${CLOUD_SQL_INSTANCE:?CLOUD_SQL_INSTANCE must be set}"

PROJECT="axiom-gtmvelo"
REGION="us-central1"
SERVICE="axiom-engine"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${SERVICE}/${SERVICE}"

echo "==> Building Docker image via Cloud Build (no local Docker required)..."
gcloud builds submit \
  --tag "${IMAGE}:latest" \
  --project "${PROJECT}" \
  .

echo "==> Deploying to Cloud Run (service: ${SERVICE}, project: ${PROJECT}, region: ${REGION})..."

gcloud run deploy "${SERVICE}" \
  --image "${IMAGE}:latest" \
  --project "${PROJECT}" \
  --region "${REGION}" \
  --platform managed \
  --allow-unauthenticated \
  --port 8080 \
  --memory 512Mi \
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
