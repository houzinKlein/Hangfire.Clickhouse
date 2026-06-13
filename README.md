# Hangfire.ClickHouse

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
dotnet add package Hangfire.ClickHouse
```

Targets `net10.0` and `net8.0`.

## Usage

```csharp
using Hangfire;
using Hangfire.ClickHouse;

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
