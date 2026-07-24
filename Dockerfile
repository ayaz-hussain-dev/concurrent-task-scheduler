# syntax=docker/dockerfile:1

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN useradd --create-home --uid 10001 --user-group appuser

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN python -m pip install .

COPY examples ./examples

FROM base AS test
COPY tests ./tests
CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-v"]

FROM base AS runtime
USER appuser
CMD ["python", "-m", "task_scheduler"]
