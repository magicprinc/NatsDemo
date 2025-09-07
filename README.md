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
| Operation                            |    op/s |
|:-------------------------------------|--------:|
| 1️⃣ Create 10 mi keys                | 233_459 |
| 2️⃣ Single thread sequential reads   | 544_781 |
| 3️⃣ Single thread random BATCH reads | 102_022 |
| 4️⃣ Multi threads random reads | 171_010 |

### NATS
| Operation                  |    op/s |
|:---------------------------|--------:|
| Single-Thread create keys  |   2_774 |
| Single thread random reads |   2_868 |
| Async publish no wait      | 138_773 |
| Async publish and wait     | 119_846 |
| Multi thread random reads  |  19_218 |

### Redis
| Operation                  |  op/s |
|:---------------------------|------:|
| Single-Thread create keys  | 3_858 |
| Single thread random reads | 3_884 |

### Apache Ignite (Embedded)
| Operation                  |    op/s |
|:---------------------------|--------:|
| Single-Thread create keys  | 177_147 |
| Single thread random reads | 409_668 |