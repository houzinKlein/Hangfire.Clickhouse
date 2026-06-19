# Contributing

Thanks for your interest in improving HangfireCH!

## Prerequisites
- .NET SDK 10 (the projects multi-target `net10.0` and `net8.0`).
- Docker — the integration tests spin up a real ClickHouse via [Testcontainers](https://testcontainers.com/).

## Build & test
```bash
dotnet restore
dotnet build -c Release
dotnet test -c Release
```
The test suite pulls `clickhouse/clickhouse-server:24.12` on first run. To test another version:
```bash
CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:latest dotnet test -c Release
```

## Benchmark
```bash
HANGFIRE_CLICKHOUSE="Host=localhost;Port=9000;User=default;Password=;Database=default" \
  dotnet run -c Release --project bench/HangfireCH.Benchmark -- 5000
```

## Conventions worth knowing (ClickHouse + Octonica)
These are load-bearing — see code comments and the README "Design & guarantees":
- `DateTime` parameters must use `DbType.DateTime2` (else sub-second precision is lost).
- Mutations (`DELETE`/`ALTER`) must use **server-side predicates only**, never client
  parameters (Octonica passes params as temp tables that don't survive async mutations).
- `argMax` skips NULLs — read nullable "latest value" columns with `argMax(tuple(col), ver).1`.
- Aggregate reads (`count()`) return `UInt64`; use the `ReadInt64`/`Convert` helpers.

## Pull requests
- Keep the build green: `dotnet test -c Release` must pass (all 40+ tests).
- Add/adjust tests for behavior changes. New ClickHouse-version-sensitive behavior should be
  covered by the integration tests so the CI matrix catches drift.
- Update `CHANGELOG.md` under `[Unreleased]`.
