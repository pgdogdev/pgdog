# Datadog integration

PgDog exports a lot of metrics via an OpenMetrics endpoint. You can enable the endpoint
by specifying the port number in `pgdog.toml`:

```toml
openmetrics_port = 9090
```

A sample config is included in [`openmetrics.d/conf.yaml`](openmetrics.d/conf.yaml). We've also included a Datadog dashboard
you can import in [`dashboard.json`](dashboard.json).
