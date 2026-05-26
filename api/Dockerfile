# ---- Stage 1: Build frontend ----
FROM node:22-alpine AS frontend
WORKDIR /build
COPY frontend-swedish-books/package.json frontend-swedish-books/package-lock.json ./
RUN npm ci --prefer-offline
COPY frontend-swedish-books/ .
RUN npm run build

# ---- Stage 2: Run API ----
FROM python:3.12-slim
WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dotenvx
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -sfS https://dotenvx.sh/install.sh | sh \
    && apt-get purge -y curl && rm -rf /var/lib/apt/lists/*

# Install Python deps via uv (no venv, system install)
COPY api/pyproject.toml api/uv.lock ./
RUN uv pip install --system --no-cache -r pyproject.toml

COPY api/ ./api/
COPY --from=frontend /build/dist ./static

# .env.production is injected at deploy time via fly secrets or mounted file;
# dotenvx loads it if present.
EXPOSE 8080
CMD ["dotenvx", "run", "--env-file=.env.production", "--", \
     "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
