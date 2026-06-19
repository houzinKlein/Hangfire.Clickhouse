using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Octonica.ClickHouseClient;

namespace Hangfire.ClickHouse;

/// <summary>
/// Folds raw counter deltas into the <c>aggregated_counter</c> table to keep the raw table
/// small. Guarded by a distributed lock so only one server aggregates at a time (otherwise
/// two passes would double-count the same rows). A one-second cutoff margin keeps the
/// read/delete from racing freshly-inserted deltas.
/// </summary>
internal sealed class CountersAggregator : IBackgroundProcess
{
    private const string LockResource = "locks:counters-aggregator";

    private static readonly ILog Logger = LogProvider.GetLogger(typeof(CountersAggregator));

    private readonly ClickHouseStorage _storage;

    public CountersAggregator(ClickHouseStorage storage) => _storage = storage;

    public void Execute(BackgroundProcessContext context)
    {
        try
        {
            Aggregate(context.StoppingToken);
        }
        catch (OperationCanceledException) when (context.StoppingToken.IsCancellationRequested)
        {
            // shutting down
        }

        context.Wait(_storage.Options.CountersAggregateInterval);
    }

    /// <summary>Runs a single aggregation pass. Exposed for deterministic testing.</summary>
    internal void Aggregate(CancellationToken cancellationToken = default)
    {
        var connection = new ClickHouseStorageConnection(_storage);

        IDisposable lockHandle;
        try
        {
            lockHandle = connection.AcquireDistributedLock(LockResource, TimeSpan.FromSeconds(1));
        }
        catch (DistributedLockTimeoutException)
        {
            return; // another server is aggregating
        }

        using (lockHandle)
        {
            var schema = _storage.Schema;

            _storage.UseConnection(c =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                var batches = new List<(string Key, long Total, DateTime? Expire)>();
                using (var read = c.CreateCommand(
                    $@"SELECT key, sum(value) AS total, max(expire_at) AS e FROM {schema.Counter}
                       WHERE created_at < now64(6) - toIntervalSecond(1) GROUP BY key"))
                using (var reader = read.ExecuteReader())
                {
                    while (reader.Read())
                        batches.Add((reader.GetStringOrEmpty(0), reader.ReadInt64(1), reader.GetNullableUtcDateTime(2)));
                }

                foreach (var (key, total, expire) in batches)
                {
                    c.ExecuteNonQuery(
                        $@"INSERT INTO {schema.AggregatedCounter} (key, value, expire_at)
                           VALUES ({{key:String}}, {{value:Int64}}, {{expire:Nullable(DateTime64(6))}})",
                        ("key", key), ("value", total), ("expire", expire));
                }

                // Remove the consumed rows (server-side cutoff, mutation -> no parameters).
                c.ExecuteNonQuery($"DELETE FROM {schema.Counter} WHERE created_at < now64(6) - toIntervalSecond(1)");

                if (batches.Count > 0)
                    Logger.DebugFormat("Counters aggregator folded {0} counter key(s).", batches.Count);
            });
        }
    }

    public override string ToString() => "ClickHouse Counters Aggregator";
}
