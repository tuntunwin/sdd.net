# State Driven Design — .NET Broker

A minimal, high-performance state broker for distributing desired-state across subscribers via WebSocket. Built for microservice and distributed system architectures where many services, devices, and edge nodes must converge on a shared view of desired vs. current state.

This is not a general-purpose message queue. It is a **state broker** — publishers declare the current state of a topic, subscribers receive that state reliably, and only the latest value per topic matters.

## Project Milestone

This implementation follows the **State-Driven Design** specification, which defines a two-state-vector model for distributed systems:

- **Desired state** (operator intent): retained at QoS 1, durable, replicated
- **Current state** (device-reported): retained at QoS 1, convergence tracking
- **Commands** (PTZ, ephemeral): QoS 0 fire-and-forget, sub-millisecond

The design evaluates several architectural approaches (Database-as-SST, NATS JetStream, custom broker) and selects a custom minimal broker as the zero-dependency option for constrained environments (edge deployments, <512MB RAM, no third-party binary policy).

Reference: `vmp-state-driven-design.html`

### Cross-Platform Support

The broker runs on all platforms supported by .NET 8:

| Platform | Runtime | NativeAOT | Notes |
|----------|---------|-----------|-------|
| **Windows** x64/ARM64 | Yes | Yes | Primary dev platform. `HttpListener` fully supported. |
| **Linux** x64/ARM64 | Yes | Yes | Production target. Docker scratch images (~35MB). |
| **macOS** x64/ARM64 (Apple Silicon) | Yes | Yes | Full support since .NET 6. `HttpListener` uses managed sockets (no Windows HTTP.sys dependency). |

NativeAOT produces a single self-contained binary per platform with no runtime dependency:

```bash
dotnet publish -c Release -r win-x64     -p:PublishAot=true -o dist/win-x64
dotnet publish -c Release -r linux-x64   -p:PublishAot=true -o dist/linux-x64
dotnet publish -c Release -r linux-arm64 -p:PublishAot=true -o dist/linux-arm64
dotnet publish -c Release -r osx-x64     -p:PublishAot=true -o dist/osx-x64
dotnet publish -c Release -r osx-arm64   -p:PublishAot=true -o dist/osx-arm64
```

All APIs used (`HttpListener`, `System.Net.WebSockets`, `FileStream`, `System.Threading.Channels`, `System.Text.Json`) are cross-platform BCL types. `InvariantGlobalization=true` eliminates ICU dependency on Linux/macOS. The optional `DotNext.Net.Cluster` Raft library is pure managed C# with no native dependencies.

### Implementation Progress

| Step | Component | Status |
|------|-----------|--------|
| 1 | Frame record + core interfaces | Done |
| 2 | ShardedStateStore (64-shard) | Done |
| 3 | NullWalWriter + NoopConsensus wiring | Done |
| 4 | Session Manager + Channel\<Frame\> | Done |
| 5 | WebSocket Transport (HttpListener) | Done |
| 6 | Topic Router (trie + wildcards) | Done |
| 7 | WAL Writer (batched fsync + rotation) | Done |
| 8 | QoS Engine (outbox + retry) | Done |
| 9 | Snapshots (periodic dump + recovery) | Partial (snapshot builder in Raft mode) |
| 10 | Raft Consensus (DotNext, optional) | Done |

---

## High-Level Design

### Architecture — 7 Components

```
                    +-----------+
  Client A ------->| Transport |  (HttpListener + WebSocket)
  Client B ------->|  (C1)     |
  Client C ------->+-----------+
                         |
                  +------+------+
                  |             |
            +-----+-----+ +----+----+
            |  Message   | | Session |
            |  Router    | | Manager |
            |  (C2)      | |  (C5)   |
            +-----+------+ +----+----+
                  |              |
            +-----+------+      |  Channel<Frame>
            | StateStore |      |  per connection
            |  (C3)      |      |  (256, DropOldest)
            +-----+------+      |
                  |              |
            +-----+------+ +----+----+
            |  Storage   | |  QoS    |
            |  Engine    | |  Engine |
            |  (C6)      | |  (C4)   |
            +-----+------+ +----+----+
                  |
            +-----+------+
            | Consensus  |
            |  Layer     |
            |  (C7)      |
            +------------+
```

| # | Component | .NET Primitive | Purpose |
|---|-----------|---------------|---------|
| C1 | Transport | `HttpListener` + `System.Net.WebSockets` | WebSocket accept, per-conn read/write Task loops |
| C2 | Message Router | `ReaderWriterLockSlim` + topic trie | Match topic with `+`/`#` wildcards, fan-out to subscriber set |
| C3 | StateStore | `ReaderWriterLockSlim[64]` sharded | 64-shard Dictionary KV, WAL-before-memory invariant |
| C4 | QoS Engine | `ConcurrentDictionary` + `SemaphoreSlim` | Per-client outbox (topic-keyed, latest-value-wins), retry with exponential backoff |
| C5 | Session Manager | `ConcurrentDictionary` + `Channel<Frame>` | Client-to-session map, bounded send channels (256, DropOldest) |
| C6 | Storage Engine | `FileStream` + `PeriodicTimer` + `SemaphoreSlim` | WAL append, batched fsync every 2ms, segment rotation at 64MB |
| C7 | Consensus Layer | `IConsensusLayer` interface | `NoopConsensus` (single-node) or `DotNext.Raft` (cluster) |

---

## Usage

### Run the broker

```bash
cd src/StateBroker
dotnet run
# StateBroker listening on http://localhost:8080/
#   store.mode=wal  cluster.mode=single
```

### Test client

```bash
cd src/StateBroker.TestClient

# Demo — pub/sub with retained state push
dotnet run

# Subscribe (ACKs all messages)
dotnet run -- sub "sensors/#"

# Subscribe without ACK (test retry behavior)
dotnet run -- sub-noack "sensors/#"       # never ACK
dotnet run -- sub-noack "sensors/#" 3     # ACK after skipping 3 retries

# Publish (retained by default)
dotnet run -- pub "sensors/temp"          # retained
dotnet run -- pub "sensors/temp" false    # non-retained
```

### Wire protocol

NDJSON over WebSocket. Each message is a JSON frame:

```json
{"type":"PUBLISH","topic":"sensors/temp","qos":1,"msgId":"abc123","retain":true,"payload":{"temp":22.5}}
{"type":"SUBSCRIBE","topic":"sensors/#"}
{"type":"DELIVER","topic":"sensors/temp","qos":1,"msgId":"def456","retained":true,"payload":{"temp":22.5}}
{"type":"ACK","msgId":"def456"}
{"type":"SUBACK","topic":"sensors/#"}
{"type":"PING"}
{"type":"PONG"}
{"type":"REDIRECT","payload":{"leader":"10.0.0.1:9000"}}
```

### Build

```bash
# Development
dotnet build

# With Raft cluster support
dotnet build -p:EnableRaft=true

# NativeAOT production (~15MB binary, ~5ms startup)
dotnet publish -c Release -r linux-x64 -p:PublishAot=true

# NativeAOT with Raft
dotnet publish -c Release -r linux-x64 -p:PublishAot=true -p:EnableRaft=true
```

---

## Design Considerations

### Messaging — QoS 0 and QoS 1

The broker implements two quality-of-service levels matching MQTT semantics:

**QoS 0 (Fire-and-forget):** Publish goes directly to matched subscribers via `Channel.TryWrite`. If the subscriber's channel is full (256 frames), the oldest frame is dropped (`BoundedChannelFullMode.DropOldest`). No persistence, no outbox, no retry. Used for PTZ commands and ephemeral telemetry.

**QoS 1 (At-least-once):** Publish is enqueued in a per-subscriber outbox keyed by topic. The outbox is a `Dictionary<string, Frame>` — newer values for the same topic replace older ones (latest-value-wins). A background retry loop re-sends un-ACKed frames with exponential backoff (`retryBase` 1s, doubling up to `retryMax` 1m). The client must send an ACK frame to clear the entry. Retried frames are marked with `Dup=true`.

**MQTT parity features:**
- PUBACK: broker ACKs QoS 1 publishes back to the publisher
- SUBACK: broker confirms subscription acceptance
- Retained delete: publishing empty payload with `retain=true` deletes retained state
- DUP flag: retry loop marks re-sent frames so clients can distinguish first delivery from retry
- Retained state push on subscribe: always delivered at QoS 1 with MsgId, retried until ACKed

**Key difference from message queues:** The outbox stores only the latest value per topic per subscriber. Publishing `sensors/temp=22` then `sensors/temp=23` means the outbox only holds `23`. A subscriber that reconnects gets the current truth, not a backlog of historical values.

### Concurrency — Lock-Free Where Possible

**Sharded StateStore:** 64 shards, each with its own `ReaderWriterLockSlim` + `Dictionary<string, JsonElement>`. Shard selection: `HashCode.Combine(topic) % 64`. Multiple readers can access different shards concurrently. Write locks are held only for the dictionary operation (microseconds), never across an `await`.

**Session channels:** `Channel.CreateBounded<Frame>(256)` with `SingleReader=true` hint. The write loop does `await foreach` drain. `TryWrite` is lock-free internally (interlocked CAS). Backpressure: `DropOldest` for QoS 0.

**QoS outbox:** `ConcurrentDictionary<string, Outbox>` where each `Outbox` has a `SemaphoreSlim(1,1)` (async-safe mutex). The semaphore is held only during dictionary operations, never across I/O or `Task.Delay`.

**Critical rule:** `ReaderWriterLockSlim` is NOT async-safe. The pattern is always: acquire lock, operate, release lock, then await. Violating this causes thread-pool starvation.

### StateStore — WAL + Sharded Memory

**Write path:** `SetAsync(topic, payload)`:
1. WAL append (serialize to NDJSON, write to `FileStream`)
2. Wait for batched fsync (2ms periodic timer flushes to disk, then releases all blocked writers via `SemaphoreSlim.Release(N)`)
3. Update in-memory shard under write lock

**Batched fsync:** Instead of fsync per write, a `PeriodicTimer` fires every 2ms and calls `FileStream.Flush(flushToDisk: true)`. Writers block on a `SemaphoreSlim` after appending — the flush loop releases all waiters in one batch. This is the .NET equivalent of Go's `sync.Cond.Broadcast`.

**Segment rotation:** WAL file rotated when exceeding 64MB. Sealed segments are immutable. Active file: `wal.log`, sealed: `wal-00000001.log`, etc. On replay, sealed segments are read in order, then the active file.

**Snapshot + Restart:** Iterate all 64 shards under brief per-shard read locks, serialize to JSON, atomic swap via `File.Replace`. On restart: load snapshot, replay WAL tail from the snapshot's last sequence number.

**Delete support:** Publishing empty payload with `retain=true` appends a delete entry to the WAL and removes from memory. WAL entries carry a `delete` flag for correct replay.

### Distributed Consensus — Raft

The broker supports optional Raft-based clustering via DotNext.Net.Cluster, activated by building with `-p:EnableRaft=true`. All Raft code is behind `#if ENABLE_RAFT` conditional compilation — the default build has zero NuGet dependencies.

**What goes through Raft:**
- Retained state writes (set and delete) — must be consistent across all nodes
- Proposed via `IConsensusLayer.ProposeAsync` → `RaftCluster.ReplicateAsync`

**What stays local:**
- QoS 0 commands (fire-and-forget, never touch Raft)
- Non-retained QoS 1 (transient delivery via local outbox)
- Session/connection state, subscriptions, heartbeats
- QoS delivery outbox + retry (per-node)

**Write path (Raft mode):**
1. Client publishes retained QoS 1 to any node
2. If follower: return REDIRECT frame with leader endpoint
3. If leader: serialize `StateEntry` → `ProposeAsync` → `ReplicateAsync`
4. Quorum (2/3) ACK → entry committed in Raft log
5. `BrokerStateMachine.ApplyAsync` called on all nodes: `SetMemory`/`DeleteMemory` + notify local subscribers
6. Leader sends PUBACK to publisher

**Key invariant:** `StateStore.SetMemory()` is called only from `ApplyAsync`, never from the publish handler. The Raft PersistentState IS the WAL — `NullWalWriter` is injected to avoid double durability.

**Snapshots:** `BrokerStateMachine` extends `MemoryBasedStateMachine` (DotNext v5). Snapshot creation serializes `store.GetAll()` as a JSON array. Restore calls `store.LoadBulk()`. DotNext handles snapshot transfer to new nodes via `InstallSnapshot` RPC.

**Configuration (appsettings.json for Raft):**
```json
{
  "cluster": {
    "mode": "raft",
    "localEndpoint": "10.0.0.1:9000",
    "peers": ["10.0.0.2:9000", "10.0.0.3:9000"],
    "raftElectionTimeout": "00:00:00.150",
    "raftLogDir": "./data/raft"
  }
}
```

---

## Testing

### Test suite overview

**202 tests** covering unit, integration, and concurrency scenarios.

```bash
dotnet test                                    # run all
dotnet test --filter BrokerIntegrationTests    # WebSocket integration only
dotnet test --filter TopicRouterTests          # topic matching only
dotnet test --filter QosEngineTests            # outbox + retry only
dotnet test --filter ConsensusRoutingTests     # leader/follower routing
```

### Unit tests

| Test class | Tests | Coverage |
|------------|-------|----------|
| `FrameTests` | 8 | Record construction, defaults, equality, with-expression, JSON round-trip, Dup flag, type constants |
| `WalEntryTests` | 2 | Construction, JSON round-trip with delete flag |
| `StateEntryTests` | 5 | Set/Delete ops, JSON round-trip, array serialization |
| `NullWalWriterTests` | 5 | Monotonic sequences, immediate flush, empty replay, idempotent dispose, concurrent uniqueness |
| `NoopConsensusTests` | 3 | Always leader, immediate propose, empty data, RequiresProposal=false |
| `ShardedStateStoreTests` | 36 | CRUD, overwrite, delete, GetAll, LoadBulk, SetAsync WAL integration, DeleteAsync, shard distribution, concurrent reads/writes/deletes, power-of-2 validation |
| `SessionTests` | 7 | Channel enqueue, DropOldest overflow, cancel, dispose, default buffer depth |
| `SessionManagerTests` | 12 | GetOrCreate idempotency, TryGet, TrySend, Remove, ConnectedClientIds, concurrent access |
| `TopicRouterTests` | 21 | Exact match, `+` wildcard (start/middle/end), `#` wildcard (root/nested), mixed wildcards, unsubscribe, UnsubscribeAll, `Matches()` static method (15 theory cases), duplicate subscribe, concurrency |
| `WalWriterTests` | 8 | Append+replay round-trip, monotonic sequences, batched fsync blocking, directory creation, concurrent appends, segment rotation, complex payloads, replay while writer open |
| `QosEngineTests` | 20 | Enqueue/GetPending, topic dedup (latest-value-wins), stale MsgId ACK, per-client isolation, evict, EvictTopic, concurrent enqueue+ack, retry loop (resend, stop after ACK, cancel on disconnect, latest-value-only), skip-then-ACK |
| `BrokerConfigTests` | 9 | All config record defaults, explicit overrides, JSON load, round-trip |

### Integration tests

| Test class | Tests | Coverage |
|------------|-------|----------|
| `BrokerIntegrationTests` | 26 | Real WebSocket end-to-end: connect, multi-client, ping/pong, publish retain/overwrite/ephemeral, session tracking, graceful close, subscribe+deliver fan-out, wildcard matching, retained state push (QoS 1), QoS 1 outbox+ACK, multiple subscribers, retry (un-ACKed retried with same MsgId, ACK stops retry), no duplicate on reconnect, skip-then-ACK, stale MsgId during retries, multi-topic outbox, PUBACK, SUBACK (before retained push), retained delete, DUP flag on retry |
| `ConsensusRoutingTests` | 5 | NoopConsensus properties (RequiresProposal, LeaderEndpoint), MockConsensusLayer (leader/follower, ProposeAsync recording) |
| `RedirectIntegrationTests` | 7 | Real WebSocket with mock follower: retained QoS 1 redirected, retained delete redirected, QoS 0 not redirected, non-retained QoS 1 not redirected, no PUBACK on redirect, follower doesn't store, ping works on follower |

### Test patterns

- **Concurrency tests:** Parallel Task.Run for reads/writes/deletes verifying no corruption
- **WAL tests:** Temp directories with cleanup, separate writer instances for replay verification
- **Integration tests:** Random ephemeral ports via `TcpListener(0)`, `IAsyncLifetime` for broker lifecycle, `ClientWebSocket` for real WebSocket connections
- **QoS retry tests:** Fast retry config (100ms base) for deterministic timing, drain-and-verify pattern for retry content
- **Consensus tests:** `MockConsensusLayer` simulating leader/follower with proposal recording

---

## Project Structure

```
net/
  CLAUDE.md                              # AI assistant instructions
  README.md                              # This file
  .gitignore
  doc/
    dotnet-design-plan.html              # Full architecture spec
    dotnet-raft-consensus.html           # Raft clustering layer design
    storage-comparison.html              # Storage engine evaluation
  src/
    StateBroker/
      Program.cs                         # Entrypoint + wiring
      Broker.cs                          # Accept loop, dispatch, PUBLISH/SUBSCRIBE/ACK handling
      appsettings.json                   # Default config (WAL mode)
      StateBroker.csproj                 # .NET 8, NativeAOT, conditional DotNext.Raft
      Core/
        Frame.cs                         # Wire frame record (NDJSON)
        IStateStore.cs                   # Set/Get/Delete + memory-only variants
        ShardedStateStore.cs             # 64-shard RWLS implementation
        IWalWriter.cs                    # WAL interface
        WalWriter.cs                     # Batched fsync + segment rotation
        NullWalWriter.cs                 # No-op WAL for in-memory/Raft mode
        IQosEngine.cs                    # QoS outbox interface
        QosEngine.cs                     # Topic-keyed outbox + retry loop
        IConsensusLayer.cs               # Consensus interface + NoopConsensus
        ITransport.cs                    # Transport + connection interfaces
        TopicRouter.cs                   # Trie-based topic matching
        Session.cs                       # Bounded Channel<Frame> per client
        SessionManager.cs               # Client-to-session registry
        BrokerConfig.cs                  # Config records (store, qos, session, cluster)
        BrokerJsonContext.cs             # AOT-safe JSON source generator
        StateEntry.cs                    # Raft log entry (topic + payload + op)
        WalEntry.cs                      # WAL entry (seq + topic + payload + delete)
        LeaderRedirect.cs                # REDIRECT payload
      Transport/
        WebSocketTransport.cs            # HttpListener-based WebSocket accept
        WebSocketConnection.cs           # IConnection over System.Net.WebSockets
      Raft/                              # Conditionally compiled (#if ENABLE_RAFT)
        RaftConsensusLayer.cs            # IConsensusLayer wrapping DotNext RaftCluster
        BrokerStateMachine.cs            # MemoryBasedStateMachine: Apply + Snapshot
        BrokerLogEntry.cs                # IRaftLogEntry for raw byte proposals
        RaftBootstrap.cs                 # Cluster factory (TCP transport, peer config)
        SubscriberNotifier.cs            # Sync fan-out from ApplyAsync
        ISubscriberNotifier.cs           # Notification interface
        NotLeaderException.cs            # Exception with leader endpoint
    StateBroker.Tests/                   # xUnit test project (202 tests)
    StateBroker.TestClient/              # Interactive CLI test client
```

---

## Configuration

`appsettings.json` deserialized via `System.Text.Json` source generators (no `Microsoft.Extensions.Configuration`).

| Key | Default | Description |
|-----|---------|-------------|
| `addr` | `:8080` | WebSocket listen address |
| `store.mode` | `wal` | `wal` for crash-safe, `noop` for in-memory only |
| `store.walDir` | `./data` | WAL file directory |
| `store.fsyncIntervalMs` | `2` | Batched fsync window (ms) |
| `store.shards` | `64` | StateStore shard count (power of 2) |
| `store.segmentMb` | `64` | WAL segment rotation threshold (MB) |
| `qos.retryBase` | `1s` | QoS 1 initial retry interval |
| `qos.retryMax` | `1m` | Maximum retry interval (exponential backoff cap) |
| `session.sendBufferDepth` | `256` | Bounded channel depth per session |
| `cluster.mode` | `single` | `single` or `raft` |
| `cluster.localEndpoint` | - | Raft RPC endpoint (e.g. `10.0.0.1:9000`) |
| `cluster.peers` | `[]` | Other cluster members |
| `cluster.raftElectionTimeout` | `150ms` | Raft election timeout |

---

## Hard Rules

- **Zero NuGet for core.** Only BCL APIs. Single optional dep: `DotNext.Net.Cluster` when `cluster.mode=raft`.
- **Never hold `ReaderWriterLockSlim` across an `await`.** Acquire, operate, release, then await.
- **WAL before memory.** `SetAsync` must append+fsync WAL before updating the in-memory shard.
- **NativeAOT compatible.** All JSON via `[JsonSerializable]` source generators. No reflection.
- **Raft mode replaces WAL.** `NullWalWriter` injected. `SetMemory()` called only from `ApplyAsync`.
- **QoS 0 never touches Raft.** Fire-and-forget by definition.

Target: .NET 8, C# 12, `InvariantGlobalization=true`, `AllowUnsafeBlocks=true`.
