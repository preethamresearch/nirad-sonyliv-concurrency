# Watchhouse serving layer for Cloud Run.
#
# The image is deliberately small: the dashboard is stdlib-only Python (the
# kafka/redis/OTel touches are lazy imports used by the local streaming demo)
# and the data plane is ClickHouse Cloud, which is already remote. What ships
# is the API + UI; what stays local is the edge: Kafka/Redis ingest, the
# replay producer, and pipeline runs against local dataset files.
FROM python:3.12-slim

WORKDIR /app
COPY scripts/ scripts/
COPY sql/ sql/
COPY web/ web/
COPY results/ results/
COPY README.md .

# Cloud Run injects PORT; HOST=0.0.0.0 so its health checks can reach us.
# ClickHouse credentials arrive as env vars (CH_HOST, CH_PORT, CH_USER,
# CH_PASSWORD, CH_SECURE, CH_DB) -- there is no .env in the image, by
# construction.
ENV HOST=0.0.0.0 PORT=8080 PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["python", "scripts/dashboard.py"]
