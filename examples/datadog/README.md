# Datadog integration

PgDog can export metrics to Datadog via two mechanisms:

1. OpenMetrics collector
2. OTLP (OTEL) ingestion endpoint

Both export the same metrics, at a configurable interval of your choice.

## OpenMetrics

PgDog exports a lot of metrics via an OpenMetrics endpoint. You can enable the endpoint
by specifying the port number in `pgdog.toml`:

```toml
# pgdog.toml
[general]
openmetrics_port = 9090
```

The endpoint will run on `http://0.0.0.0:9090`. A sample config is included in [`openmetrics.d/conf.yaml`](openmetrics.d/conf.yaml).


## OLTP/OTEL

PgDog can push OTEL metrics to a configurable endpoint. Datadog supports ingesting metrics this way and we support it natively:

```toml
# pgdog.toml
[otel]
datadog_api_key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
endpoint = "https://otlp.us5.datadoghq.com/v1/metrics"
```

By default, PgDog will push metrics to the endpoint every 10 seconds.

## Dashboard

We have a pre-build Datadog dashboard you can import directly into your stack. See [dashboard.json](dashboard.json).
