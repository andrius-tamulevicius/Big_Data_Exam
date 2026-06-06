FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

RUN mkdir -p data/raw data/results /tmp/ais/temp /tmp/ais/filtered /tmp/ais/results /tmp/spark-temp

CMD ["spark-submit", "--master", "local[2]", "--driver-memory", "3g", "--conf", "spark.driver.maxResultSize=768m", "--conf", "spark.sql.shuffle.partitions=32", "--conf", "spark.default.parallelism=32", "--conf", "spark.sql.adaptive.enabled=true", "--conf", "spark.local.dir=/tmp/spark-temp", "/app/src/main.py"]
