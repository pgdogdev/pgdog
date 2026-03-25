---
name: Something is broken
about: Something isn't working as expected
title: ''
labels: ''
assignees: ''
type: Bug

---

**PgDog version**
e.g., `v0.1.34` or `a41e41d1`

**Description**
What happened and what should have happened instead.

**Logs**
Attach trace logs, if the issue can be reproduced locally. To get trace logs, set `RUST_LOG=trace`, e.g.:

```bash
export RUST_LOG=trace
cargo run
```

**Configuration**
Include relevant (or all) PgDog settings from `pgdog.toml` and `users.toml`. Redact passwords and other sensitive info.
