# NatsDemo

## Key-Value Store benchmarks

All numbers are operations per second (op/s).
The underscore (_) is a thousand separator.

### SQLite

| Operation                 |      op/s |
|:--------------------------|----------:|
| batch insert              |   441_345 |
| single thread random read |    58_888 |
| select table              | 1_108_156 |
| multi thread random read  |    58_665 |

### RocksDB
| Operation                            |    op/s |
|:-------------------------------------|--------:|
| 1️⃣ Create 10 mi keys                | 233_459 |
| 2️⃣ Single thread sequential reads   | 544_781 |
| 3️⃣ Single thread random BATCH reads | 102_022 |
| 4️⃣ Multi threads random reads |