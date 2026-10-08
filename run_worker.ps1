$env:EVENT_COLLECTOR_KAFKA_BROKERS="localhost:9094"
$env:EVENT_COLLECTOR_CH_ADDRS="localhost:9000"
$env:EVENT_COLLECTOR_CH_DATABASE="argus_analytics"
$env:EVENT_COLLECTOR_PG_HOST="localhost"
$env:EVENT_COLLECTOR_PG_PORT="5435"
$env:EVENT_COLLECTOR_PG_DATABASE="argus"
$env:EVENT_COLLECTOR_PG_USERNAME="argus"
$env:EVENT_COLLECTOR_PG_PASSWORD="argus_secret"
$env:EVENT_COLLECTOR_JWT_SIGNING_KEY="2xp8vaqUMGpY3uYRHGpMWLUqin9rfqy154qIaWPtay8="
$env:EVENT_COLLECTOR_REDIS_ADDR="localhost:6379"
$env:EVENT_COLLECTOR_INFERENCE_PYTHON_BRIDGE_URL="http://localhost:8091"
$env:EVENT_COLLECTOR_MINIO_ENDPOINT="localhost:9002"
$env:EVENT_COLLECTOR_MINIO_ACCESS_KEY="argus-minio-admin"
$env:EVENT_COLLECTOR_MINIO_SECRET_KEY="argus-minio-secret"
$env:EVENT_COLLECTOR_MINIO_BUCKET="argus-evidence"
$env:MODEL_FAIL_FAST="false"

cd argus-backend
go run cmd/worker/main.go -config ../argus-infra/docker/config.yaml

