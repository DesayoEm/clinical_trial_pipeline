FROM python:3.12-alpine

WORKDIR /app

RUN apk add --no-cache \
    postgresql-dev \
    gcc \
    musl-dev \
    dcron \
    && rm -rf /var/cache/apk/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl /app/etl
COPY dbt_studies /app/dbt_studies
COPY config.py /app/config.py

ENV PYTHONPATH=/app

RUN mkdir -p /app/data/shards /app/data/compacted /app/etl/states /var/log && \
    chmod -R 777 /app/data /app/etl/states /var/log


RUN mkdir -p /app/data/shards /app/data/compacted /app/etl/states /var/log && \
    chmod -R 777 /app/data /app/etl/states /var/log && \
    echo 'result = ""' > /app/etl/states/last_extraction_result.py && \
    echo 'shard_path = ""' > /app/etl/states/last_shard_path.py && \
    echo 'last_saved_token = ""' > /app/etl/states/last_token.py

CMD ["python", "-m", "etl.main"]
