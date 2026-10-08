FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY pyproject.toml MANIFEST.in README.md LICENSE PROVENANCE.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir .
COPY examples ./examples

CMD ["python", "-m", "examples.runtime", "client", "--from-environment"]
