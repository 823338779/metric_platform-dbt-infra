FROM ghcr.io/astral-sh/uv:0.12.17 AS uv

FROM python:3.12-slim AS builder
COPY --from=uv /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY vendor/dbt/core ./vendor/dbt/core
COPY vendor/metricflow ./vendor/metricflow
COPY vendor/dbt-metricflow/requirements-files ./vendor/dbt-metricflow/requirements-files
COPY vendor/dbt-metricflow/dbt-metricflow ./vendor/dbt-metricflow/dbt-metricflow
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev

FROM python:3.12-slim AS runtime
RUN useradd --create-home --uid 10001 app
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY pyproject.toml ./
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src" \
    PYTHONUNBUFFERED="1"
USER app
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=2)"]
CMD ["dbt-metricflow-service"]
