# Streamlit Dashboard — Production Container

# WHY python:3.11-slim AS THE BASE?
#   - Official, security-patched Python image
#   - "slim" variant is ~120 MB vs ~900 MB for full python:3.11
#   - Smaller image = faster ECR push, faster Fargate cold start, lower ECR cost
#   - 3.11 matches our local dev environment, avoiding subtle version drift

# LAYER ORDER (cache-friendly):
#   1. Base image
#   2. System deps (rarely change)
#   3. Python deps (change occasionally)
#   4. App code (changes often)

FROM python:3.11-slim

# Environment
# PYTHONDONTWRITEBYTECODE       — don't litter .pyc files (smaller image, faster start)
# PYTHONUNBUFFERED              — print() and log output reach CloudWatch immediately
# PIP_NO_CACHE_DIR              — don't keep pip's download cache (smaller image)
# PIP_DISABLE_PIP_VERSION_CHECK — quieter builds
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps
# curl is needed for the health check. Clean apt lists in the same RUN
# so they never land in an image layer.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Python deps — copied separately for cache efficiency.
# If only app code changes (the common case), this layer stays cached.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code — copied LAST so code changes don't bust the pip cache.
COPY app.py athena_client.py ./

# Run as a non-root user. If the app were ever compromised, the attacker
# would not have root inside the container.
RUN useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

# Network
# Streamlit listens on 8501. EXPOSE is documentation only; the port is
# actually published by `docker run -p` locally or the task definition on ECS.
EXPOSE 8501

# Health check
# Streamlit's built-in health endpoint. Used by `docker run` locally.
# NOTE: ECS ignores Dockerfile HEALTHCHECK — on Fargate the equivalent check
# must be declared in the task definition's `healthCheck` block.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Run
# --server.address=0.0.0.0
#   Streamlit binds to 127.0.0.1 by default, which is unreachable from outside
#   a container. 0.0.0.0 binds to all interfaces so Fargate can route traffic in.
# --server.headless=true
#   Non-interactive mode: Streamlit won't try to open a browser (which would
#   fail inside a container).
# --browser.gatherUsageStats=false
#   Disables Streamlit's anonymous telemetry. Optional, cleaner for production.
CMD ["streamlit", "run", "app.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]