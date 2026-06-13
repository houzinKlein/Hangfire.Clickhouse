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
