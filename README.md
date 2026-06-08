# dagster-exporter

Prometheus exporter for [Dagster](https://dagster.io/). Queries the Dagster GraphQL API and exposes run counts, schedule status, and sensor status as Prometheus metrics.

## Metrics

| Metric | Labels | Description |
|--------|--------|-------------|
| `dagster_runs_total` | `status` | Number of runs by status (`QUEUED`, `STARTED`, `SUCCESS`, `FAILURE`, `CANCELING`, `CANCELED`) |
| `dagster_run_last_duration_seconds` | `job_name`, `status` | Duration in seconds of the most recent run per job and status |
| `dagster_schedule_running` | `schedule_name`, `repository` | `1` if the schedule is running, `0` if stopped |
| `dagster_schedule_last_tick_timestamp_seconds` | `schedule_name`, `repository` | Unix timestamp of the schedule's last tick |
| `dagster_sensor_running` | `sensor_name`, `repository` | `1` if the sensor is running, `0` if stopped |
| `dagster_sensor_last_tick_timestamp_seconds` | `sensor_name`, `repository` | Unix timestamp of the sensor's last tick |

## Configuration

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `-dagster-url` | `DAGSTER_URL` | `http://localhost:3000` | Dagster webserver URL |
| `-listen-address` | `LISTEN_ADDRESS` | `:8000` | Address to expose metrics on |
| `-runs-limit` | — | `200` | Number of recent completed runs to fetch for duration metrics |

## Usage

### Binary

```bash
go build -o dagster-exporter .
DAGSTER_URL=http://localhost:3000 ./dagster-exporter
```

### Docker

```bash
docker build -t dagster-exporter .
docker run -e DAGSTER_URL=http://dagster-webserver:3000 -p 8000:8000 dagster-exporter
```

### Build and push to ECR

```bash
# Login to ECR
make ecr-login

# Build for linux/amd64 (default)
make build TAG=v0.1.0

# Push to ECR
make push TAG=v0.1.0

# Build and push in one step
make release TAG=v0.1.0

# Re-tag a released version as latest (same digest, no rebuild)
make tag-latest TAG=v0.1.0
```

Override variables as needed:

| Variable | Default | Description |
|----------|---------|-------------|
| `TAG` | `latest` | Image tag |
| `PLATFORM` | `linux/amd64` | Target platform |
| `REGION` | `ap-northeast-1` | AWS region for ECR login |

### docker-compose (sidecar)

```yaml
dagster-exporter:
  image: dagster-exporter
  environment:
    DAGSTER_URL: "http://webserver.dagster.local:3000"
  ports:
    - "8000:8000"
  depends_on:
    - webserver
```

### Prometheus scrape config

```yaml
scrape_configs:
  - job_name: dagster
    static_configs:
      - targets: ['localhost:8000']
```

## Endpoints

- `GET /metrics` — Prometheus metrics
- `GET /healthz` — Health check
