# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/) (pre-1.0: minor versions may carry breaking changes).

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
