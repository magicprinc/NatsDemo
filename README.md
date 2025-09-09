# NatsDemo

## Key-Value Store benchmarks
https://github.com/nats-io/nats.go/discussions/1507#discussioncomment-14312986

All numbers are operations per second (op/s).
The underscore (_) is a thousand separator.

### SQLite (local library: no network)
| Operation                 |      op/s |
|:--------------------------|----------:|
| batch insert              |   441_345 |
| single thread random read |    58_888 |
| select table              | 1_108_156 |
| multi thread random read  |    58_665 |

### RocksDB (local library: no network)
| Operation                        |    op/s |
|:---------------------------------|--------:|
| Create 10 mi keys                | 233_459 |
| Create 10 mi keys Batch 5k       | 1_472_320 |
| Create 10 mi keys Multi thread   | 1_888_931 |
| Single thread sequential reads   | 544_781 |
| Single thread random BATCH reads | 102_022 |
| Multi thread random reads        | 171_010 |

### NATS in Docker
| Operation                  |    op/s |
|:---------------------------|--------:|
| Single-Thread create keys  |   2_774 |
| Single thread random reads |   2_868 |
| Async publish no wait      | 138_773 |
| Async publish and wait     | 119_846 |
| Multi thread random reads  |  19_218 |

### NATS without Docker (host OS)
| Operation                  |    op/s |
|:---------------------------|--------:|
| Single-Thread create keys  |  14_468 |
| Single thread random reads |  15_152 |
| Async publish no wait      | 269_978 |
| Async publish and wait     | 155_715 |
| Multi thread random reads  |  89_009 |

### Redis in Docker
| Operation                  |  op/s |
|:---------------------------|------:|
| Single-Thread create keys  | 3_858 |
| Single thread random reads | 3_884 |

### Apache Ignite (Embedded)
| Operation                  |    op/s |
|:---------------------------|--------:|
| Single-Thread create keys  | 177_147 |
| Single thread random reads | 409_668 |

### MySQL NDB Cluster with ClusterJ
⌛