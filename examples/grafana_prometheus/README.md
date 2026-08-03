# Grafana + Prometheus

Pushes PgDog metrics to Prometheus over OTLP and visualizes them in Grafana.

```
pgdog  --OTLP push (every 5s)-->  prometheus  <--  grafana
```

## Push, not scrape

PgDog's OTEL exporter (`[otel]` block in `pgdog.toml`) POSTs OTLP JSON to
Prometheus's built-in OTLP receiver, enabled with `--web.enable-otlp-receiver`
(see `docker-compose.metrics.yml`). `prometheus.yml` has no scrape jobs —
Prometheus only ingests what PgDog pushes.

Metric names are prefixed with `pgdog_` (via `namespace` in `[otel]`); OTLP
attributes become Prometheus labels.

## Grafana provisioning

`provisioning/grafana/` is mounted into `/etc/grafana/provisioning/`, so the
Prometheus datasource and the PgDog dashboard show up automatically on first
boot.

## Running

```sh
docker compose up
```

- Prometheus — http://127.0.0.1:9090
- Grafana — http://127.0.0.1:3000 (admin / admin), _PgDog_ folder
