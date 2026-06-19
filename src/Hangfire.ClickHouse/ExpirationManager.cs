using System;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Server;
using Octonica.ClickHouseClient;

namespace Hangfire.ClickHouse;

/// <summary>
/// Periodically removes expired and soft-deleted rows. All cleanup uses server-side
/// predicates and subqueries only (no client parameters), because Octonica passes
/// parameters as temporary tables that don't exist when an async mutation runs.
/// </summary>
internal sealed class ExpirationManager : IBackgroundProcess
{
    private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

    private readonly ClickHouseStorage _storage;

    public ExpirationManager(ClickHouseStorage storage) => _storage = storage;

    public void Execute(BackgroundProcessContext context)
    {
        try
        {
            Prune(context.StoppingToken);
        }
        catch (OperationCanceledException) when (context.StoppingToken.IsCancellationRequested)
        {
            // shutting down
        }

        context.Wait(_storage.Options.JobExpirationCheckInterval);
    }

    /// <summary>Runs a single cleanup pass. Exposed for deterministic testing.</summary>
    internal void Prune(CancellationToken cancellationToken = default)
    {
        var s = _storage.Schema;

        // job_expiration drives which jobs are expired; it must be pruned LAST so the
        // cascade subqueries can still see the expired ids.
        var expiredJobs = $@"SELECT job_id FROM (
                SELECT job_id, argMax(tuple(expire_at), ver).1 AS e FROM {s.JobExpiration} GROUP BY job_id
            ) WHERE e IS NOT NULL AND e < now64(6)";

        var statements = new[]
        {
            $"DELETE FROM {s.Job} WHERE id IN ({expiredJobs})",
            $"DELETE FROM {s.JobState} WHERE job_id IN ({expiredJobs})",
            $"DELETE FROM {s.JobParameter} WHERE job_id IN ({expiredJobs})",
            $"DELETE FROM {s.State} WHERE job_id IN ({expiredJobs})",
            $"DELETE FROM {s.JobExpiration} WHERE job_id IN ({expiredJobs})",

            // Collections: drop every version of a key whose current state is removed or expired.
            $@"DELETE FROM {s.Set} WHERE (key, value) IN (
                   SELECT key, value FROM (
                       SELECT key, value, argMax(removed, ver) AS r, argMax(tuple(expire_at), ver).1 AS e
                       FROM {s.Set} GROUP BY key, value
                   ) WHERE r = 1 OR (e IS NOT NULL AND e < now64(6)))",
            $@"DELETE FROM {s.Hash} WHERE (key, field) IN (
                   SELECT key, field FROM (
                       SELECT key, field, argMax(removed, ver) AS r, argMax(tuple(expire_at), ver).1 AS e
                       FROM {s.Hash} GROUP BY key, field
                   ) WHERE r = 1 OR (e IS NOT NULL AND e < now64(6)))",
            $@"DELETE FROM {s.List} WHERE (key, id) IN (
                   SELECT key, id FROM (
                       SELECT key, id, argMax(removed, ver) AS r, argMax(tuple(expire_at), ver).1 AS e
                       FROM {s.List} GROUP BY key, id
                   ) WHERE r = 1 OR (e IS NOT NULL AND e < now64(6)))",

            // Counters.
            $"DELETE FROM {s.Counter} WHERE expire_at IS NOT NULL AND expire_at < now64(6)",
            $"DELETE FROM {s.AggregatedCounter} WHERE expire_at IS NOT NULL AND expire_at < now64(6)",

            // Queue: drop removed entries.
            $@"DELETE FROM {s.JobQueue} WHERE (queue, job_id) IN (
                   SELECT queue, job_id FROM (
                       SELECT queue, job_id, argMax(removed, ver) AS r FROM {s.JobQueue} GROUP BY queue, job_id
                   ) WHERE r = 1)",

            // Servers: drop removed servers.
            $@"DELETE FROM {s.Server} WHERE id IN (
                   SELECT id FROM (SELECT id, argMax(removed, ver) AS r FROM {s.Server} GROUP BY id) WHERE r = 1)",

            // Distributed locks: drop released or expired locks.
            $@"DELETE FROM {s.DistributedLock} WHERE resource IN (
                   SELECT resource FROM (
                       SELECT resource, argMax(released, ver) AS rel, argMax(tuple(expire_at), ver).1 AS e
                       FROM {s.DistributedLock} GROUP BY resource
                   ) WHERE rel = 1 OR e < now64(6))",
        };

        _storage.UseConnection(connection =>
        {
            // Apply the configured lightweight-delete sync level for this connection.
            connection.ExecuteNonQuery($"SET lightweight_deletes_sync = {_storage.Options.MutationsSync}");

            foreach (var statement in statements)
            {
                cancellationToken.ThrowIfCancellationRequested();
                connection.ExecuteNonQuery(statement);
            }
        });

        if (_storage.Options.UseKeeperMap)
        {
            // Reap expired KeeperMap entries: stale lock owners and claims left by done jobs
            // (whose job_queue row is removed above) or crashed workers.
            _storage.UseConnection(connection =>
            {
                connection.ExecuteNonQuery($"DELETE FROM {s.DistributedLockKeeper} WHERE expire_at < now64(6)");
                connection.ExecuteNonQuery($"DELETE FROM {s.QueueClaimKeeper} WHERE expire_at < now64(6)");
            });
        }

        Logger.Debug("ClickHouse expiration manager completed a cleanup pass.");
    }

    public override string ToString() => "ClickHouse Expiration Manager";
}
