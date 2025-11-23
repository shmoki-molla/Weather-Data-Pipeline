FROM apache/superset:latest

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        pkg-config \
        libzstd-dev && \
    rm -rf /var/lib/apt/lists/*

USER superset
WORKDIR /app

RUN pip cache purge && \
    pip uninstall -y sqlalchemy-clickhouse clickhouse-sqlalchemy clickhouse-driver

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt --verbose

