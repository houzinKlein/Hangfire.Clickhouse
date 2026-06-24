# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/) (pre-1.0: minor versions may carry breaking changes).

## [0.3.2]

### Changed (hardening against server-side setting defaults)
- Pin **`insert_deduplicate = 0`** per connection. On `Replicated*MergeTree` (clusters), ClickHouse
  drops "duplicate" insert blocks; the append-only counter deltas and version rows must never be
  silently de-duplicated. No-op on single-node `MergeTree`.
- Pin **`session_timezone = 'UTC'`** per connection so `now64()`/timestamp handling stays
  deterministic regardless of the server's local time zone (all columns are `DateTime64('UTC')`).
- Dashboard's analytical `JobState FINAL` scans now run with **`max_execution_time = 0`** so a strict
  server-side limit can't abort the dashboard on large tables. The hot lock/queue paths keep the
  server default.

## [0.3.1]

### Fixed
- **`Unknown table expression identifier '_…' … While executing WaitForAsyncInsert`** on servers
  that enable `async_insert` by default (a profile/user setting). Octonica sends query parameters as
  a temporary table that no longer exists when ClickHouse flushes a deferred async insert, so every
  parameterized insert (distributed lock, jobs, hashes, …) failed. The storage now forces
  `async_insert = 0` on each connection (via the connection factory's open hook and the schema
  installer), independent of the server default. Regression-tested against a server configured with
  `async_insert=1`.

## [0.3.0]

### Changed
- **Published NuGet package ID is now `HGF.ClickHouse`.** The `Hangfire.*` ID prefix is reserved
  on nuget.org, so neither `Hangfire.ClickHouse` nor a dotted variant could be listed. Install with
  `dotnet add package HGF.ClickHouse`.
- **Assembly and namespace renamed to `HangfireCH`** (were `Hangfire.ClickHouse`). Update
  `using Hangfire.ClickHouse;` to `using HangfireCH;`; `UseClickHouseStorage(...)` is still found via
  `using Hangfire;`. Breaking, but pre-1.0.

## [0.2.1]

### Changed
- **NuGet package ID is now `HangfireCH`.** The `Hangfire.*` ID prefix is reserved on
  nuget.org, so `Hangfire.ClickHouse` could not be listed. The assembly name and code
  namespace remain `Hangfire.ClickHouse`, so consuming code (`using Hangfire.ClickHouse;`,
  `UseClickHouseStorage(...)`) is unchanged — only the `PackageReference` ID changes.

## [Unreleased]

### Added
- **Client-side write batching** (`BatchWrites`, default on): a transaction's inserts into the
  same table are sent as one multi-row `INSERT`, producing fewer/larger MergeTree parts.
- **Partitioning** of the append/queue tables (`job`, `job_queue`, `state`, `counter`) by
  insert-time month for pruning and lighter merges.
- **KeeperMap path** (`UseKeeperMap`, opt-in): linearizable distributed lock and atomic queue
  claim backed by ClickHouse Keeper. Falls back to the optimistic path when disabled.
- New options: `BatchWrites`, `UseKeeperMap`, `KeeperMapPathPrefix`, `ConnectionPoolSize`,
  `MutationsSync`; connection-string and option validation with clearer errors.
- Diagnostics: storage/expiration/aggregator logging via Hangfire `ILog`, and
  `WriteOptionsToLog`.
- Faster dashboard: current-state reads use ReplacingMergeTree `FINAL`.
- Concurrency tests, a throughput benchmark (`bench/`), and a CI ClickHouse version matrix.

### Changed
- Schema version bumped to 2 (partitioning + optional KeeperMap tables). Fresh installs only;
  existing databases are not auto-migrated.

### Notes
- `async_insert` is intentionally **not** used: Octonica passes parameters as temporary tables
  that don't survive ClickHouse's deferred async-insert execution. Batching is done client-side
  instead.
- Per-job expiration is dynamic, so cleanup stays `DELETE`-based; `DROP PARTITION` is not used
  for job retention (it would drop still-live old jobs).

## [0.1.0]

Initial preview: full Hangfire storage SPI on ClickHouse via the Octonica client —
`JobStorage`, connection, write-only transaction, monitoring API, polling job queue, and
background maintenance (expiration, counter aggregation). Requires ClickHouse 24.12+.
