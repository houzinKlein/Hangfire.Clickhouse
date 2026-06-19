using System;

namespace Hangfire.ClickHouse;

/// <summary>
/// Configuration for <see cref="ClickHouseStorage"/>.
/// </summary>
public class ClickHouseStorageOptions
{
    private TimeSpan _queuePollInterval;
    private TimeSpan _invisibilityTimeout;
    private TimeSpan _jobExpirationCheckInterval;
    private TimeSpan _countersAggregateInterval;
    private TimeSpan _distributedLockExpiration;
    private int _connectionPoolSize;
    private int _mutationsSync;

    public ClickHouseStorageOptions()
    {
        QueuePollInterval = TimeSpan.FromSeconds(15);
        InvisibilityTimeout = TimeSpan.FromMinutes(30);
        JobExpirationCheckInterval = TimeSpan.FromMinutes(30);
        CountersAggregateInterval = TimeSpan.FromMinutes(5);
        DistributedLockExpiration = TimeSpan.FromMinutes(30);
        PrepareSchemaIfNecessary = true;
        TablePrefix = string.Empty;
        DatabaseName = null;
        BatchWrites = true;
        UseKeeperMap = false;
        KeeperMapPathPrefix = "/";
        ConnectionPoolSize = 32;
        MutationsSync = 1;
    }

    /// <summary>
    /// Optional database the storage tables live in. When <c>null</c> the database
    /// from the connection string is used. The database is created when missing.
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Prefix applied to every storage table name (e.g. <c>hf_</c>). Empty by default.
    /// </summary>
    public string TablePrefix { get; set; }

    /// <summary>When <c>true</c>, the schema is created/verified on startup.</summary>
    public bool PrepareSchemaIfNecessary { get; set; }

    /// <summary>
    /// When <c>true</c> (default), a write transaction's inserts into the same table are sent
    /// as a single multi-row <c>INSERT</c> on commit, producing fewer (larger) MergeTree parts
    /// and less merge pressure than one statement per row. (ClickHouse <c>async_insert</c> can't
    /// be used here: Octonica passes parameters as temporary tables that don't survive the
    /// deferred async-insert execution.)
    /// </summary>
    public bool BatchWrites { get; set; }

    /// <summary>
    /// When <c>true</c>, the distributed lock and the queue claim use the linearizable
    /// <c>KeeperMap</c> engine (requires ClickHouse Keeper / ZooKeeper to be configured on
    /// the server). This upgrades the lock to true mutual exclusion and the dequeue to an
    /// atomic claim. When <c>false</c> (default), the optimistic best-effort path is used.
    /// </summary>
    public bool UseKeeperMap { get; set; }

    /// <summary>
    /// ZooKeeper/Keeper path prefix for <c>KeeperMap</c> tables. Must match (be under) the
    /// server's configured <c>keeper_map_path_prefix</c>. Only used when <see cref="UseKeeperMap"/>
    /// is <c>true</c>. Default <c>/</c>.
    /// </summary>
    public string KeeperMapPathPrefix { get; set; }

    /// <summary>Maximum number of pooled ClickHouse connections. Default 32.</summary>
    public int ConnectionPoolSize
    {
        get => _connectionPoolSize;
        set
        {
            if (value <= 0) throw new ArgumentException("ConnectionPoolSize must be positive.", nameof(ConnectionPoolSize));
            _connectionPoolSize = value;
        }
    }

    /// <summary>
    /// Synchronization level applied to the expiration manager's lightweight deletes
    /// (<c>lightweight_deletes_sync</c>): 0 = async, 1 = wait on the current replica
    /// (default), 2 = wait on all replicas.
    /// </summary>
    public int MutationsSync
    {
        get => _mutationsSync;
        set
        {
            if (value is < 0 or > 2) throw new ArgumentException("MutationsSync must be 0, 1, or 2.", nameof(MutationsSync));
            _mutationsSync = value;
        }
    }

    /// <summary>How often the job queue is polled for new work.</summary>
    public TimeSpan QueuePollInterval
    {
        get => _queuePollInterval;
        set
        {
            ThrowIfNegativeOrZero(value, nameof(QueuePollInterval));
            _queuePollInterval = value;
        }
    }

    /// <summary>
    /// How long a fetched job stays invisible to other workers. If the owning worker
    /// dies without removing or requeueing the job, it becomes eligible again after
    /// this period (at-least-once recovery).
    /// </summary>
    public TimeSpan InvisibilityTimeout
    {
        get => _invisibilityTimeout;
        set
        {
            ThrowIfNegativeOrZero(value, nameof(InvisibilityTimeout));
            _invisibilityTimeout = value;
        }
    }

    /// <summary>How often expired records are removed by the expiration manager.</summary>
    public TimeSpan JobExpirationCheckInterval
    {
        get => _jobExpirationCheckInterval;
        set
        {
            ThrowIfNegativeOrZero(value, nameof(JobExpirationCheckInterval));
            _jobExpirationCheckInterval = value;
        }
    }

    /// <summary>How often raw counter rows are folded into the aggregated counter table.</summary>
    public TimeSpan CountersAggregateInterval
    {
        get => _countersAggregateInterval;
        set
        {
            ThrowIfNegativeOrZero(value, nameof(CountersAggregateInterval));
            _countersAggregateInterval = value;
        }
    }

    /// <summary>
    /// Time-to-live applied to an acquired distributed lock. Guards against
    /// dead-lock when a lock owner crashes; the lock auto-expires after this period.
    /// </summary>
    public TimeSpan DistributedLockExpiration
    {
        get => _distributedLockExpiration;
        set
        {
            ThrowIfNegativeOrZero(value, nameof(DistributedLockExpiration));
            _distributedLockExpiration = value;
        }
    }

    private static void ThrowIfNegativeOrZero(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new ArgumentException($"The {name} property value should be positive.", name);
    }
}
