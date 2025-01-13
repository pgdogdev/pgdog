# Config

`SHOW CONFIG` is a command to show currently loaded values from [`pgdog.toml`](../configuration/pgdog.toml/general.md). For example:

```
admin=> SHOW CONFIG;
           name            |     value
---------------------------+----------------
 ban_timeout               | 5m
 default_pool_size         | 10
 healthcheck_interval      | 30s
 host                      | 0.0.0.0
 idle_healthcheck_delay    | 5s
 idle_healthcheck_interval | 30s
 load_balancing_strategy   | random
 min_pool_size             | 1
 pooler_mode               | transaction
 port                      | 6432
 rollback_timeout          | 5s
 shutdown_timeout          | 5s
 tls_certificate           | not configured
 tls_private_key           | not configured
 workers                   | 2
(15 rows)
```
