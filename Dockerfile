# Build stage: install the package and its dependencies into a virtual environment.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS build
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

# Runtime stage.
FROM python:3.13-slim-bookworm
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    # Make processor modules mounted next to the server config importable.
    PYTHONPATH="/config/processors"

# The whitelist and the file paths served over HTTP are resolved relative to the
# working directory, so run from the mounted config directory.
WORKDIR /config

EXPOSE 3000 50051

ENTRYPOINT ["rdfc-runner-server"]
CMD ["/config/server.ttl"]
