# ── Stage 1: Build dependencies
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build tools needed for asyncpg and lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Production image
FROM python:3.11-slim

WORKDIR /app

# Runtime libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libxml2 \
    libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Copy application code
COPY . .

# Cloud Run requires the app to listen on $PORT (default 8080)
ENV PORT=8080

# Non-root user for security
RUN useradd -m -u 1001 axiom
USER axiom

EXPOSE 8080

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
