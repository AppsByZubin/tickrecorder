FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY tickrecorder ./tickrecorder
RUN python -m pip install --upgrade pip \
    && python -m pip install .

COPY main.py ./

RUN mkdir -p /app/data /app/logs/fyers-sdk

CMD ["python", "-m", "tickrecorder"]

