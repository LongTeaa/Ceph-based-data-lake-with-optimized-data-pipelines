# Monitoring

Phase 6 adds a local Prometheus and Grafana stack for observing the demo
pipeline while storage, Spark, Airflow, and query benchmarks are running.

## Services

The local monitoring stack is defined in `docker/compose.yml`.

| Service | URL | Purpose |
|---|---|---|
| Prometheus | <http://localhost:9090> | Scrapes metrics from local services. |
| Grafana | <http://localhost:3000> | Displays provisioned dashboards. |
| statsd-exporter | <http://localhost:9102/metrics> | Converts Airflow StatsD metrics to Prometheus. |

Grafana uses the credentials from `.env`:

```dotenv
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin
```

## Start and Stop

Start the local metric targets and dashboards:

```bash
make monitoring-up
```

This starts MinIO, Spark master/worker, statsd-exporter, Prometheus, and
Grafana. If you want Airflow task metrics, start Airflow as well:

```bash
make airflow-up
```

Stop only the monitoring services:

```bash
make monitoring-down
```

Follow logs:

```bash
make monitoring-logs
```

## Scrape Targets

Prometheus is configured in `docker/prometheus/prometheus.yml`.

| Job | Endpoint | Notes |
|---|---|---|
| `minio` | `minio:9000/minio/v2/metrics/cluster` | Uses public local metrics through `MINIO_PROMETHEUS_AUTH_TYPE=public`. |
| `spark-master` | `spark-master:8080/metrics/master/prometheus` | Enabled by `docker/spark/metrics.properties`. |
| `spark-worker` | `spark-worker:8081/metrics/prometheus` | Enabled by `docker/spark/metrics.properties`. |
| `airflow` | `statsd-exporter:9102/metrics` | Airflow emits StatsD metrics to statsd-exporter. |
| `prometheus` | `prometheus:9090/metrics` | Prometheus self-scrape. |

Check target health at:

```text
http://localhost:9090/targets
```

## Dashboard

Grafana provisions one dashboard from git:

```text
docker/grafana/dashboards/data-lake-local-overview.json
```

The dashboard includes:

- scrape target health;
- MinIO storage usage;
- MinIO S3 request rate;
- Spark master/worker metrics;
- Airflow task success/failure counters when Airflow has emitted StatsD metrics.

Open it from Grafana under the `Data Lake` folder:

```text
Data Lake Local Overview
```

## Current Limits

This Phase 6 implementation is for the local Docker Compose environment.
Ceph-specific metrics will require a real Ceph RGW/cluster exporter in the
benchmark environment. Trino is health-checked by Docker Compose and benchmark
results, but this local stack does not yet add a Trino JMX exporter.
