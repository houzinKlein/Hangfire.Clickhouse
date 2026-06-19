using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient;

namespace Hangfire.ClickHouse;

/// <summary>
/// Resolves fully-qualified, prefixed table names for a storage instance.
/// </summary>
internal sealed class ClickHouseSchema
{
    private readonly string _database;
    private readonly string _prefix;
    private readonly string _keeperPrefix;

    public ClickHouseSchema(string database, string prefix, string keeperMapPathPrefix = "/")
    {
        _database = database;
        _prefix = prefix ?? string.Empty;
        _keeperPrefix = string.IsNullOrEmpty(keeperMapPathPrefix) ? "/" : keeperMapPathPrefix.TrimEnd('/');
        if (_keeperPrefix.Length == 0) _keeperPrefix = ""; // root
    }

    public string Database => _database;

    /// <summary>Quoted, database-qualified table name, e.g. <c>`hangfire`.`hf_job`</c>.</summary>
    public string Table(string name) => $"`{_database}`.`{_prefix}{name}`";

    /// <summary>ZooKeeper/Keeper path for a KeeperMap-backed table.</summary>
    public string KeeperPath(string name) => $"{_keeperPrefix}/{_database}/{_prefix}{name}";

    public string Schema => Table("schema");
    public string Job => Table("job");
    public string JobState => Table("job_state");
    public string JobExpiration => Table("job_expiration");
    public string JobParameter => Table("job_parameter");
    public string State => Table("state");
    public string JobQueue => Table("job_queue");
    public string Server => Table("server");
    public string Hash => Table("hash");
    public string List => Table("list");
    public string Set => Table("set");
    public string Counter => Table("counter");
    public string AggregatedCounter => Table("aggregated_counter");
    public string DistributedLock => Table("distributed_lock");

    // KeeperMap-backed tables (only created when UseKeeperMap is enabled).
    public string DistributedLockKeeper => Table("distributed_lock_kv");
    public string QueueClaimKeeper => Table("queue_claim_kv");
}

/// <summary>
/// Creates the ClickHouse schema (database + tables) used by the storage.
/// </summary>
internal static class ClickHouseObjectsInstaller
{
    public const int SchemaVersion = 2;

    public static async Task InstallAsync(ClickHouseConnection connection, ClickHouseSchema schema, bool useKeeperMap = false, CancellationToken cancellationToken = default)
    {
        // The database itself.
        await ExecuteAsync(connection, $"CREATE DATABASE IF NOT EXISTS `{schema.Database}`", cancellationToken).ConfigureAwait(false);

        foreach (var statement in CreateStatements(schema))
        {
            await ExecuteAsync(connection, statement, cancellationToken).ConfigureAwait(false);
        }

        if (useKeeperMap)
        {
            foreach (var statement in KeeperMapStatements(schema))
            {
                await ExecuteAsync(connection, statement, cancellationToken).ConfigureAwait(false);
            }
        }

        // Record the installed schema version (idempotent; we only ever read max(version)).
        await ExecuteAsync(connection,
            $"INSERT INTO {schema.Schema} (version) VALUES ({SchemaVersion})", cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// KeeperMap-backed tables for the linearizable lock and queue claim. Requires the server
    /// to have <c>keeper_map_path_prefix</c> configured (and Keeper/ZooKeeper available).
    /// </summary>
    public static IEnumerable<string> KeeperMapStatements(ClickHouseSchema s)
    {
        yield return $@"CREATE TABLE IF NOT EXISTS {s.DistributedLockKeeper} (
            resource String,
            owner String,
            expire_at DateTime64(6,'UTC')
        ) ENGINE = KeeperMap('{s.KeeperPath("distributed_lock")}') PRIMARY KEY resource";

        yield return $@"CREATE TABLE IF NOT EXISTS {s.QueueClaimKeeper} (
            job_id String,
            queue String,
            owner String,
            enqueued_at DateTime64(6,'UTC'),
            expire_at DateTime64(6,'UTC')
        ) ENGINE = KeeperMap('{s.KeeperPath("queue_claim")}') PRIMARY KEY job_id";
    }

    public static IEnumerable<string> CreateStatements(ClickHouseSchema s)
    {
        // Schema version marker.
        yield return $@"CREATE TABLE IF NOT EXISTS {s.Schema} (
            version Int32,
            installed_at DateTime64(3,'UTC') DEFAULT now64(3)
        ) ENGINE = MergeTree ORDER BY version";

        // Immutable job definition (inserted once; ver guards against accidental duplicates).
        // Partitioned by creation month for pruning and smaller merges. NOTE: per-job retention
        // is dynamic (set via job_expiration), so expiration stays DELETE-based — DROP PARTITION
        // can't be used here without dropping still-live old jobs.
        yield return $@"CREATE TABLE IF NOT EXISTS {s.Job} (
            id String,
            invocation_data String,
            arguments String,
            created_at DateTime64(6,'UTC'),
            ver UInt64
        ) ENGINE = ReplacingMergeTree(ver) PARTITION BY toYYYYMM(created_at) ORDER BY id";

        // Current state pointer (mutated by inserting a newer version).
        yield return $@"CREATE TABLE IF NOT EXISTS {s.JobState} (
            job_id String,
            state_name String,
            ver UInt64
        ) ENGINE = ReplacingMergeTree(ver) ORDER BY job_id";

        // Current expiration pointer. NULL = persisted (never expires).
        yield return $@"CREATE TABLE IF NOT EXISTS {s.JobExpiration} (
            job_id String,
            expire_at Nullable(DateTime64(6,'UTC')),
            ver UInt64
        ) ENGINE = ReplacingMergeTree(ver) ORDER BY job_id";

        // Job parameters.
        yield return $@"CREATE TABLE IF NOT EXISTS {s.JobParameter} (
            job_id String,
            name String,
            value Nullable(String),
            ver UInt64
        ) ENGINE = ReplacingMergeTree(ver) ORDER BY (job_id, name)";

        // Append-only state history. ver gives a total order for "latest state" reads.
        yield return $@"CREATE TABLE IF NOT EXISTS {s.State} (
            id String,
            job_id String,
            name String,
            reason Nullable(String),
            created_at DateTime64(6,'UTC'),
            data String,
            ver UInt64
        ) ENGINE = MergeTree PARTITION BY toYYYYMM(created_at) ORDER BY (job_id, created_at)";

        // Queue entries (claimed by inserting a newer version with a fetch token).
        yield return $@"CREATE TABLE IF NOT EXISTS {s.JobQueue} (
            queue String,
            job_id String,
            enqueued_at DateTime64(6,'UTC'),
            fetched_at Nullable(DateTime64(6,'UTC')),
            fetch_token String,
            removed UInt8,
            ver UInt64
        ) ENGINE = ReplacingMergeTree(ver) PARTITION BY toYYYYMM(enqueued_at) ORDER BY (queue, job_id)";

        // Servers.
        yield return $@"CREATE TABLE IF NOT EXISTS {s.Server} (
            id String,
            data String,
            last_heartbeat DateTime64(6,'UTC'),
            removed UInt8,
            ver UInt64
        ) ENGINE = ReplacingMergeTree(ver) ORDER BY id";

        // Hashes.
        yield return $@"CREATE TABLE IF NOT EXISTS {s.Hash} (
            key String,
            field String,
            value Nullable(String),
            expire_at Nullable(DateTime64(6,'UTC')),
            removed UInt8,
            ver UInt64
        ) ENGINE = ReplacingMergeTree(ver) ORDER BY (key, field)";

        // Lists (one row per element, identified by a generated id).
        yield return $@"CREATE TABLE IF NOT EXISTS {s.List} (
            key String,
            id String,
            value Nullable(String),
            created_at DateTime64(6,'UTC'),
            expire_at Nullable(DateTime64(6,'UTC')),
            removed UInt8,
            ver UInt64
        ) ENGINE = ReplacingMergeTree(ver) ORDER BY (key, id)";

        // Sets.
        yield return $@"CREATE TABLE IF NOT EXISTS {s.Set} (
            key String,
            value String,
            score Float64,
            expire_at Nullable(DateTime64(6,'UTC')),
            removed UInt8,
            ver UInt64
        ) ENGINE = ReplacingMergeTree(ver) ORDER BY (key, value)";

        // Raw counter deltas (summed at read time). created_at lets the aggregator fold a
        // stable batch without racing freshly-inserted rows.
        yield return $@"CREATE TABLE IF NOT EXISTS {s.Counter} (
            key String,
            value Int64,
            expire_at Nullable(DateTime64(6,'UTC')),
            created_at DateTime64(6,'UTC') DEFAULT now64(6)
        ) ENGINE = MergeTree PARTITION BY toYYYYMM(created_at) ORDER BY key";

        // Folded counters: SummingMergeTree collapses same-key rows; reads still sum() to
        // cover rows not yet merged.
        yield return $@"CREATE TABLE IF NOT EXISTS {s.AggregatedCounter} (
            key String,
            value Int64,
            expire_at Nullable(DateTime64(6,'UTC'))
        ) ENGINE = SummingMergeTree(value) ORDER BY key";

        // Distributed locks.
        yield return $@"CREATE TABLE IF NOT EXISTS {s.DistributedLock} (
            resource String,
            owner String,
            acquired_at DateTime64(6,'UTC'),
            expire_at DateTime64(6,'UTC'),
            released UInt8,
            ver UInt64
        ) ENGINE = ReplacingMergeTree(ver) ORDER BY resource";
    }

    private static async Task ExecuteAsync(ClickHouseConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
