Use primary for reads:

| Read/write split                | `read_only = false`, eligible replica | `read_only = false`, no eligible replica | `read_only = true`, eligible replica | `read_only = true`, no eligible replica |
| ------------------------------- | ------------------------------------- | ---------------------------------------- | ------------------------------------ | --------------------------------------- |
| `IncludePrimary`                | Yes                                   | Yes                                      | No                                   | Yes                                     |
| `IncludePrimaryIfReplicaLag`    | No                                    | Yes                                      | No                                   | Yes                                     |
| `IncludePrimaryIfReplicaBanned` | Conditional                           | Conditional                              | No                                   | Conditional                             |
| `ExcludePrimary`                | No                                    | Filtered                                 | No                                   | Filtered                                |
| `PreferPrimary`                 | No                                    | Yes                                      | No                                   | Yes                                     |

**Conditional** means at least one remaining replica is banned or exactly one candidate remains (assumed primary). A banned primary does not count.

**Filtered** means no replica remains after the resharding-only and LSN filters. Banned replicas still count, so `ExcludePrimary` returns `AllReplicasDown` when every replica is banned.
