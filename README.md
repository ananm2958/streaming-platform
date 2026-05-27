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

All TCP messages use a **4-byte big-endian length prefix** followed by a payload.
Payload fields are separated by ASCII `0x1F` so message bodies may contain spaces.

### Client requests (payload inside frame)

| Command | Payload fields |
|---------|----------------|
| `METADATA` | `METADATA` |
| `PRODUCE` | `PRODUCE`, topic, partition, message, offset |
| `FETCH` | `FETCH`, topic, partition, offset |

Example payload for produce:

```text
PRODUCE\x1forders\x1f0\x1fhello world\x1f0
```

### Responses (payload inside frame)

| Status | Payload fields |
|--------|----------------|
| OK | `OK`, offset |
| MESSAGE | `MESSAGE`, body |
| ERROR | `ERROR`, reason |

### Leader discovery

```text
METADATA
```

Returns:

```text
MESSAGE 1:127.0.0.1:9092 role:leader self:1
```

`PRODUCE` sent to a follower is **forwarded to the leader** automatically.

Inter-broker messages (`REPLICATE`, `ACK`, `COMMIT`, `HEARTBEAT`) use the same framing.
