Use primary for reads:

| Read/write split                | `read_only = false`, eligible replica | `read_only = false`, no eligible replica | `read_only = true`, eligible replica | `read_only = true`, no eligible replica |
| ------------------------------- | ------------------------------------- | ---------------------------------------- | ------------------------------------ | --------------------------------------- |
| `IncludePrimary`                | Yes                                   | Yes                                      | No                                   | Yes                                     |
| `IncludePrimaryIfReplicaLag`    | No                                    | Yes                                      | No                                   | Yes                                     |
| `IncludePrimaryIfReplicaBanned` | Conditional                           | Conditional                              | No                                   | Conditional                             |
| `ExcludePrimary`                | No                                    | Yes                                      | No                                   | Yes                                     |
| `PreferPrimary`                 | No                                    | Yes                                      | No                                   | Yes                                     |

**Conditional** means at least one remaining candidate is banned or exactly one candidate remains (assumed primary).
