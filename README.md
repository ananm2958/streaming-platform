# streaming-platform

A minimal multi-broker log with leader-based replication.

## Build

```bash
make
make test
```

## Run a 3-broker cluster

Terminal 1 (leader):

```bash
./broker --id 1 --port 9092 --host 127.0.0.1 --role leader \
  --peers 2:127.0.0.1:9093,3:127.0.0.1:9094
```

Terminal 2 (follower):

```bash
./broker --id 2 --port 9093 --host 127.0.0.1 --role follower --leader-id 1 \
  --peers 1:127.0.0.1:9092,3:127.0.0.1:9094
```

Terminal 3 (follower):

```bash
./broker --id 3 --port 9094 --host 127.0.0.1 --role follower --leader-id 1 \
  --peers 1:127.0.0.1:9092,2:127.0.0.1:9093
```

## Wire protocol

All TCP messages use a **4-byte big-endian length prefix** followed by a binary
payload. Payloads begin with a one-byte protocol version and one-byte command
code; strings use a four-byte big-endian length followed by raw bytes. This
allows message bodies to contain arbitrary bytes (including NUL and separators).

### Client requests (payload inside frame)

| Command | Binary fields |
|---------|----------------|
| `METADATA` | `METADATA` |
| `PRODUCE` | `PRODUCE`, topic, partition, message, offset |
| `BATCH_PRODUCE` | `BATCH_PRODUCE`, topic, partition, message-count, messages... |
| `FETCH` | `FETCH`, topic, partition, offset |
| `JOIN_GROUP` | `JOIN_GROUP`, group, member, topic |
| `GROUP_ASSIGNMENT` | `GROUP_ASSIGNMENT`, group, member, topic |
| `GROUP_FETCH` | `GROUP_FETCH`, group, member, topic, partition, ignored-offset |
| `COMMIT_OFFSET` | `COMMIT_OFFSET`, group, member, topic, partition, processed-offset |
| `LEAVE_GROUP` | `LEAVE_GROUP`, group, member, topic |

`BATCH_PRODUCE` appends records in request order to one partition and returns
their offsets in `appended_offsets`. If a batch result is lost or a failure
occurs after partial success, retrying can append duplicates; producers should
use idempotent consumers or application record IDs where duplicates matter.

## Storage durability

Each topic partition is an append-only log split into size-bounded segment
files. An in-memory offset index maps every logical offset to its segment and
byte position and is rebuilt when a broker starts. Appends accumulate in a
per-partition WAL. Every 64 records (or on explicit flush/shutdown), the broker
syncs the WAL, then the segment, and only then clears the WAL. On startup, an
unfinished WAL is replayed when needed; incomplete segment tails are discarded
and recovered from that WAL.

### Responses (payload inside frame)

Responses are also binary: version, success flag, response kind, offset,
length-delimited message, and a count plus zero or more batch offsets. A
successful fetch carries its record in the message field; an error carries its
reason there. Do not send the textual names from the tables as wire payloads.

### Leader discovery

Send the binary `METADATA` command (version `1`, command code `6`).

Returns:

The response message contains `1:127.0.0.1:9092 role:leader self:1`.

`PRODUCE` sent to a follower is **forwarded to the leader** automatically.

### Consumer groups

Consumer-group requests are forwarded to the leader, which owns membership and
offset state. Members join a named group and receive an `assignment:0,2` style
`MESSAGE` response. Assignments are deterministic round-robin across the
topic's existing partitions, so one partition is owned by only one member at a
time. Call `GROUP_ASSIGNMENT` after a rebalance to obtain the current set.

`GROUP_FETCH` reads the group’s next committed offset for its assigned
partition. After processing the returned record, send `COMMIT_OFFSET` with its
offset; this advances only that group/topic/partition cursor. Thus partitions
can be consumed in parallel while each partition remains strictly ordered.
Until the commit succeeds, the same record remains eligible for redelivery.
This provides **at-least-once delivery** (including after a consumer failure),
so consumers must make processing idempotent. Offsets are kept in memory for
the broker process lifetime.

Inter-broker messages (`REPLICATE`, `ACK`, `COMMIT`, `HEARTBEAT`) use the same framing.

## Replication and failover

Leaders replicate each append synchronously and acknowledge a producer only
after a majority has acknowledged the record. Brokers exchange heartbeats every
two seconds. If the recorded leader misses the heartbeat timeout, all surviving
members select the lowest broker ID among the live fixed membership as the new
leader. This deterministic rule provides failover without an external
coordinator for the configured, fixed membership.

## Performance tests

`tests/performance_test` reports localhost TCP throughput for one 256-record
batch of 512-byte messages. `tests/storage_test` also reports segment-write
throughput for 256 512-byte records and verifies index recovery after a
restart. These are measurements rather than pass/fail thresholds, since disk
and host performance vary.
