# HangfireCH

[![CI](https://github.com/houzinKlein/Hangfire.Clickhouse/actions/workflows/ci.yml/badge.svg)](https://github.com/houzinKlein/Hangfire.Clickhouse/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/HangfireCH.svg)](https://www.nuget.org/packages/HangfireCH)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

ClickHouse job storage for [Hangfire](https://www.hangfire.io/). It implements the full
Hangfire storage SPI (`JobStorage`, connection, write-only transaction, monitoring API),
a polling job queue, and background maintenance (record expiration and counter
aggregation), on top of the [Octonica ClickHouse client](https://github.com/Octonica/ClickHouseClient).

> **Heads up — ClickHouse is an OLAP column store, not an OLTP database.** It has no
> ACID transactions, no row-level locks, and `UPDATE`/`DELETE` are asynchronous
> mutations. This provider therefore models mutable state with append-only inserts into
> `ReplacingMergeTree` tables (latest version wins, read with `argMax`) and treats the job
> queue and distributed locks as **best-effort / at-least-once**. That matches Hangfire's
> own delivery model (jobs should be idempotent; Hangfire re-dispatches after a worker
> times out), but if you need strict exactly-once OLTP semantics, a transactional store
> such as SQL Server, PostgreSQL or Redis is a better fit. See
> [Design & guarantees](#design--guarantees).

## Install

```
dotnet add package HangfireCH
```

Targets `net10.0` and `net8.0`. The package ID, assembly, and namespace are all **`HangfireCH`**
(the `Hangfire.*` prefix is reserved on nuget.org). Use `using HangfireCH;` for the types and
`UseClickHouseStorage(...)` (the extension lives in the `Hangfire` namespace).

## Usage

```csharp
using Hangfire;
using HangfireCH;

GlobalConfiguration.Configuration
    .UseClickHouseStorage("Host=localhost;Port=9000;User=default;Database=hangfire");

// ASP.NET Core
builder.Services.AddHangfire(cfg => cfg
    .UseClickHouseStorage(
        "Host=localhost;Port=9000;User=default;Database=hangfire",
        new ClickHouseStorageOptions
        {
            QueuePollInterval         = TimeSpan.FromSeconds(5),
            InvisibilityTimeout       = TimeSpan.FromMinutes(30),
            JobExpirationCheckInterval = TimeSpan.FromMinutes(30),
            CountersAggregateInterval  = TimeSpan.FromMinutes(5),
        }));

builder.Services.AddHangfireServer();
```

The connection string is the Octonica format (native protocol, default port **9000**):
`Host=…;Port=9000;User=…;Password=…;Database=…`.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `DatabaseName` | from connection string | Database the tables live in; created if missing. |
| `TablePrefix` | `""` | Prefix applied to every table name. |
| `PrepareSchemaIfNecessary` | `true` | Create/verify the schema on startup. |
| `QueuePollInterval` | `15s` | How often the queue is polled for work. |
| `InvisibilityTimeout` | `30m` | How long a fetched job stays invisible before recovery. |
| `JobExpirationCheckInterval` | `30m` | Expiration manager run interval. |
| `CountersAggregateInterval` | `5m` | Counter aggregation interval. |
| `DistributedLockExpiration` | `30m` | TTL for an acquired distributed lock (dead-lock guard). |
| `BatchWrites` | `true` | Send a transaction's same-table inserts as one multi-row `INSERT` (fewer MergeTree parts). |
| `UseKeeperMap` | `false` | Use the linearizable `KeeperMap` engine for the lock + queue claim (requires ClickHouse Keeper). |
| `KeeperMapPathPrefix` | `/` | Keeper path prefix for `KeeperMap` tables; must match the server's `keeper_map_path_prefix`. |
| `ConnectionPoolSize` | `32` | Max pooled ClickHouse connections. |
| `MutationsSync` | `1` | `lightweight_deletes_sync` level for the expiration manager (0=async, 1=current, 2=all). |

## Design & guarantees

* **Jobs / state / parameters / expiration** are stored across `ReplacingMergeTree`
  tables keyed by id. A "mutation" (state change, expire, persist, set parameter) is a
  plain `INSERT` of a new version row carrying a monotonic `ver`; reads resolve the
  current value with `argMax(col, ver)`, so they are correct even before background
  merges collapse old versions.
* **State history** is an append-only `state` table.
* **Queue**: dequeue is an optimistic claim — select the oldest visible entry, insert a
  claim row stamped with a unique token, then read back the winning token. Combined with
  `InvisibilityTimeout`, a crashed worker's job becomes visible again. Under heavy
  contention this is **at-least-once** (a job may rarely be handed to two workers); keep
  jobs idempotent.
* **Distributed locks** use the same optimistic-claim pattern with a TTL, so they are
  best-effort mutual exclusion rather than a hard lock.
* **Stronger guarantees (opt-in):** set `UseKeeperMap = true` (and configure ClickHouse
  Keeper + `keeper_map_path_prefix` on the server) to back the lock and the queue claim with
  the linearizable `KeeperMap` engine — true mutual exclusion and an atomic dequeue. The
  storage falls back to the optimistic path when this is off.
* **Writes** are batched per transaction (`BatchWrites`) into multi-row inserts to limit
  MergeTree part growth. `async_insert` is deliberately not used — Octonica passes parameters
  as temporary tables that don't survive ClickHouse's deferred async-insert execution.
* **Partitioning:** `job`, `job_queue`, `state`, and `counter` are partitioned by insert-time
  month. Per-job retention is dynamic, so expiration stays `DELETE`-based (no `DROP PARTITION`,
  which would drop still-live old jobs).
* **Expiration**: every expirable record carries `expire_at`. A native ClickHouse `TTL`
  reclaims space on merges, and the expiration manager additionally issues lightweight
  `DELETE`s on the configured interval.
* **Counters** are append-only deltas summed at read time; the counter aggregator folds
  them into an `aggregated_counter` table under a storage-wide distributed lock.

## Schema

All tables are created in `DatabaseName` (or the connection's database) with `TablePrefix`:
`schema`, `job`, `job_state`, `job_expiration`, `job_parameter`, `state`, `job_queue`,
`server`, `hash`, `list`, `set`, `counter`, `aggregated_counter`, `distributed_lock`.

## Development & tests

Integration tests run against a real ClickHouse spun up with
[Testcontainers](https://testcontainers.com/) (Docker required):

```
dotnet test
```

## License

MIT
