ARG PYTHON_IMAGE=python:3.12.11-slim-bookworm@sha256:519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7

FROM ${PYTHON_IMAGE} AS builder
ARG UV_VERSION=0.11.0
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app
RUN pip install --no-cache-dir "uv==${UV_VERSION}"
COPY pyproject.toml uv.lock README.md /app/
RUN uv sync --locked --no-dev --no-install-project
COPY src /app/src
RUN uv sync --locked --no-dev --no-editable

FROM ${PYTHON_IMAGE} AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PATH=/opt/venv/bin:$PATH
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY config /app/config
RUN useradd --create-home --uid 10001 collector
USER collector
EXPOSE 8095
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=12 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8095/health', timeout=2)"]
CMD ["uvicorn", "edgeops_collector_simulation.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8095"]
