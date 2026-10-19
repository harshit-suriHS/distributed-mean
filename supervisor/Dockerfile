# Build context: the repo root (the supervisor installs the worker package it starts).
FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1
WORKDIR /app/supervisor
COPY worker /app/worker
COPY supervisor/pyproject.toml supervisor/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY supervisor/src ./src
RUN uv sync --frozen --no-dev
RUN useradd --system --uid 10001 app
USER app
# SIGTERM drains every worker (each finishes and reports its current task)
STOPSIGNAL SIGTERM
CMD ["/app/supervisor/.venv/bin/dmean-supervisor"]
