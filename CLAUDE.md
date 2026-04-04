# State Driven Design — .NET Broker

## What This Is

A minimal state broker (not a message queue) that distributes desired-state across subscribers via WebSocket. Ported from a Go reference design with structural parity. Two QoS levels: fire-and-forget (0) and at-least-once retained state (1).

## Project Structure

```
net/
  doc/                        # HTML design specs (read-only reference)
    dotnet-design-plan.html   # Full architecture, Go→.NET mapping, all components
    dotnet-raft-consensus.html # Optional Raft clustering layer
    storage-comparison.html   # Storage engine evaluation (custom WAL wins)
  src/                        # Implementation (empty — being built)
```

## Architecture — 7 Components

| # | Component | .NET Primitive | Purpose |
|---|-----------|---------------|---------|
| C1 | Transport | HttpListener + System.Net.WebSockets | WebSocket accept, per-conn read/write Task loops |
| C2 | Message Router | ReaderWriterLockSlim + topic trie | Match topic → fan-out to subscriber set |
| C3 | StateStore | ReaderWriterLockSlim[64] sharded | 64-shard Dictionary KV, WAL-before-memory |
| C4 | QoS Engine | ConcurrentDictionary + SemaphoreSlim | Per-client outbox, retry with exponential backoff |
| C5 | Session Manager | ConcurrentDictionary + Channel\<Frame\> | Client→session map, bounded send channels (256, DropOldest) |
| C6 | Storage Engine | FileStream + PeriodicTimer + SemaphoreSlim | WAL append, batched fsync every 2ms, segment rotation at 64MB |
| C7 | Consensus Layer | IConsensusLayer interface | NoopConsensus (single-node) or DotNext.Raft (cluster) |

## Hard Rules

- **Zero NuGet for core.** Only BCL APIs. Single optional dep: `DotNext.Net.Cluster` when `cluster.mode=raft`, gated by MSBuild condition.
- **Never hold ReaderWriterLockSlim across an await.** Acquire → operate → release → then await.
- **WAL before memory.** StateStore.SetAsync must append+fsync WAL before updating the in-memory shard.
- **NativeAOT compatible.** All JSON types must use `[JsonSerializable]` on a `JsonSerializerContext`. No reflection, no `dynamic`, no `Assembly.Load`.
- **Async all I/O.** Any method touching network, disk, or Channel must be `async Task`/`ValueTask`.
- **Raft mode replaces WAL.** When Raft is enabled, inject `NullWalWriter`. The Raft PersistentState IS the WAL. `StateStore.SetMemory()` is called only from `ApplyAsync`, never from the publish handler.
- **QoS 0 never touches Raft.** Fire-and-forget by definition.

## Key Patterns

- **Batched fsync:** PeriodicTimer every 2ms → `Flush(flushToDisk:true)` → `SemaphoreSlim.Release(N)` wakes all blocked writers (Go `sync.Cond.Broadcast` equivalent)
- **Sharding:** `HashCode.Combine(topic) & 63` selects shard. Each shard has its own RWLS + Dictionary.
- **Session channels:** `Channel.CreateBounded<Frame>(256, DropOldest, SingleReader:true)`. Write loop does `await foreach` drain → `WebSocket.SendAsync`.
- **Snapshots:** Iterate 64 shards under brief per-shard read locks → write temp → `File.Replace` atomic swap. Every 5 min.
- **Segment rotation:** WAL file rotated at 64MB. Sealed files immutable. Poll `FileInfo.Length` after flush.

## Wire Protocol

NDJSON over WebSocket. Frame record:
```
Type: PUBLISH | SUBSCRIBE | ACK | DELIVER | PING | PONG | REDIRECT
Topic, Qos (0|1), MsgId, Retain, Retained, Payload (JsonElement)
```

## Core Interfaces

```
IStateStore        — SetAsync, Get, GetAll
IWalWriter         — AppendAsync, WaitFlushedAsync, ReplayAsync (IAsyncDisposable)
ITransport         — AcceptAsync → IAsyncEnumerable<IConnection>
IConnection        — ClientId, ReadFramesAsync, SendAsync, CloseAsync
IQosEngine         — EnqueueAsync, AckAsync, GetPending, EvictAsync
IConsensusLayer    — ProposeAsync, IsLeader
```

## Build

```bash
# NativeAOT (production — ~15MB binary, ~5ms startup)
dotnet publish -c Release -r linux-x64 -p:PublishAot=true

# With Raft support
dotnet publish -c Release -r linux-x64 -p:PublishAot=true -p:EnableRaft=true
```

Target: .NET 8, C# 12, `InvariantGlobalization=true`, `AllowUnsafeBlocks=true` (for Span WAL ops).

## Config

`appsettings.json` deserialized directly via `System.Text.Json` — no `Microsoft.Extensions.Configuration`.

Key settings: `store.mode` (wal|noop), `store.fsyncIntervalMs` (2), `store.shards` (64), `qos.retryBase` (1s), `session.sendBufferDepth` (256), `cluster.mode` (single|raft).

## Implementation Order

1. Frame record + all interfaces
2. StateStore (64-shard in-memory)
3. NullWalWriter + NoopConsensus (get in-memory working)
4. Session Manager + Channel\<Frame\> per connection
5. Transport (HttpListener + WebSocket)
6. Message Router (topic trie + wildcard)
7. WalWriter (append + batched fsync + rotation)
8. QoS Engine (outbox + retry)
9. Snapshots (periodic dump + restart recovery)
10. Raft integration (DotNext, optional)
