# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# OS deps for psycopg/cryptography
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev libffi-dev libssl-dev curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Install Python deps first (better caching)
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# App code (compose will also mount)
COPY app/ app/

# Default command is provided by docker-compose per service
