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

# PYTHONDONTWRITEBYTECODE — don't litter .pyc files (smaller image, faster start)
# PYTHONUNBUFFERED       — print() and log output reach CloudWatch immediately
# PIP_NO_CACHE_DIR       — don't keep pip's download cache (smaller image)
# PIP_DISABLE_PIP_VERSION_CHECK — quieter builds
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps

# pyarrow needs a working build environment for some platforms. On slim, we
# install only what's strictly needed and clean up apt lists in the same
# RUN to keep the layer small.

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*


# Working directory

# Everything below operates here. ECS task logs and process listings will
# reflect this path, which makes debugging easier.
WORKDIR /app


# Python deps — copied separately for cache efficiency

# If only requirements.txt changes, this layer rebuilds and pip reinstalls.
# If only app code changes (the common case), this layer is cached and skipped.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Application code

# Copy LAST so code changes don't bust the (expensive) pip-install cache.
COPY app.py athena_client.py ./


# Network + healthcheck

# Streamlit listens on 8501 by default. EXPOSE is informational (it documents
# intent; it does not actually publish the port — that happens at run time).
EXPOSE 8501

# Streamlit's built-in health endpoint. Fargate uses this to decide whether
# the task is healthy and should keep receiving traffic.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
# WHY --server.address=0.0.0.0?
# By default Streamlit binds to 127.0.0.1 (localhost only). Inside a container
# that means nothing from outside can reach it. 0.0.0.0 binds to all network
# interfaces, which is required for Fargate to route traffic to the task.
#
# WHY --server.headless=true?
# Tells Streamlit it's running in a non-interactive environment. Stops it
# from trying to open a browser, which would crash in a container.
#
# WHY --browser.gatherUsageStats=false?
# Disables Streamlit's anonymous telemetry. Optional but cleaner for production.
CMD ["streamlit", "run", "app.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]