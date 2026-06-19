using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Hangfire;
using Hangfire.Storage;
using Octonica.ClickHouseClient;
using Octonica.ClickHouseClient.Exceptions;

namespace Hangfire.ClickHouse;

/// <summary>
/// Linearizable distributed lock and queue claim backed by the KeeperMap engine (opt-in via
/// <see cref="ClickHouseStorageOptions.UseKeeperMap"/>). Acquisition is an atomic strict-mode
/// INSERT: it succeeds for exactly one caller and throws "Node exists" for the rest — no
/// read-back race like the optimistic path. A failed strict INSERT closes the connection, so
/// every attempt uses a fresh pooled connection (broken ones are discarded on return).
/// </summary>
internal static class ClickHouseKeeperMap
{
    // ---------- distributed lock ----------

    public static IDisposable AcquireLock(ClickHouseStorage storage, string resource, TimeSpan timeout)
    {
        var owner = Guid.NewGuid().ToString("N");
        var deadline = DateTime.UtcNow + timeout;
        var sleep = 50;

        while (true)
        {
            if (TryInsertLock(storage, resource, owner))
                return new LockHandle(storage, resource, owner);

            TakeoverExpiredLock(storage, resource);

            if (DateTime.UtcNow >= deadline)
                throw new DistributedLockTimeoutException(resource);

            Thread.Sleep(sleep);
            sleep = Math.Min(sleep * 2, 1000);
        }
    }

    private static bool TryInsertLock(ClickHouseStorage storage, string resource, string owner)
    {
        var expireAt = DateTime.UtcNow.Add(storage.Options.DistributedLockExpiration);
        try
        {
            storage.UseConnection(connection =>
            {
                connection.ExecuteNonQuery("SET keeper_map_strict_mode = 1");
                connection.ExecuteNonQuery(
                    $@"INSERT INTO {storage.Schema.DistributedLockKeeper} (resource, owner, expire_at)
                       VALUES ({{r:String}}, {{o:String}}, {{e:DateTime64(6)}})",
                    ("r", resource), ("o", owner), ("e", expireAt));
            });
            return true;
        }
        catch (ClickHouseServerException)
        {
            // "Node exists" (held) or any transient server/connection error on the strict INSERT:
            // we only ever acquire on a clean success, so treat every failure as "not acquired".
            return false;
        }
    }

    private static void TakeoverExpiredLock(ClickHouseStorage storage, string resource)
    {
        try
        {
            storage.UseConnection(connection =>
            {
                connection.ExecuteNonQuery("SET keeper_map_strict_mode = 0");
                connection.ExecuteNonQuery(
                    $"DELETE FROM {storage.Schema.DistributedLockKeeper} WHERE resource = {{r:String}} AND expire_at < {{now:DateTime64(6)}}",
                    ("r", resource), ("now", DateTime.UtcNow));
            });
        }
        catch { /* best effort */ }
    }

    private static void ReleaseLock(ClickHouseStorage storage, string resource, string owner)
    {
        try
        {
            storage.UseConnection(connection =>
            {
                connection.ExecuteNonQuery("SET keeper_map_strict_mode = 0");
                connection.ExecuteNonQuery(
                    $"DELETE FROM {storage.Schema.DistributedLockKeeper} WHERE resource = {{r:String}} AND owner = {{o:String}}",
                    ("r", resource), ("o", owner));
            });
        }
        catch { /* best effort; a stuck lock is reclaimed via takeover once expired */ }
    }

    private sealed class LockHandle : IDisposable
    {
        private readonly ClickHouseStorage _storage;
        private readonly string _resource;
        private readonly string _owner;
        private int _disposed;

        public LockHandle(ClickHouseStorage storage, string resource, string owner)
        {
            _storage = storage;
            _resource = resource;
            _owner = owner;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            ReleaseLock(_storage, _resource, _owner);
        }
    }

    // ---------- queue claim ----------

    public static ClickHouseFetchedJob? TryClaim(ClickHouseStorage storage, string[] queues)
    {
        if (queues.Length == 0) return null;

        var owner = Guid.NewGuid().ToString("N");
        var schema = storage.Schema;
        var (inClause, inParameters) = BuildInClause(queues);

        // Pick the oldest queue entry that is neither removed nor actively claimed.
        string? queue = null, jobId = null;
        DateTime enqueuedAt = default;
        storage.UseConnection(connection =>
        {
            using var command = connection.CreateCommand(
                $@"SELECT jq.queue, jq.job_id, jq.enqueued_at FROM (
                       SELECT queue, job_id, argMax(removed, ver) AS removed, min(enqueued_at) AS enqueued_at
                       FROM {schema.JobQueue} WHERE queue IN ({inClause}) GROUP BY queue, job_id
                   ) AS jq
                   LEFT JOIN {schema.QueueClaimKeeper} AS c ON jq.job_id = c.job_id
                   WHERE jq.removed = 0 AND (c.job_id = '' OR c.expire_at < now64(6))
                   ORDER BY jq.enqueued_at ASC LIMIT 1");
            foreach (var (name, value) in inParameters) command.AddParameter(name, value);
            using var reader = command.ExecuteReader();
            if (reader.Read())
            {
                queue = reader.GetString(0);
                jobId = reader.GetString(1);
                enqueuedAt = reader.GetUtcDateTime(2);
            }
        });

        if (queue is null || jobId is null) return null;

        var expireAt = DateTime.UtcNow.Add(storage.Options.InvisibilityTimeout);
        try
        {
            storage.UseConnection(connection =>
            {
                // Clear a stale claim (crashed owner) in non-strict mode, then atomically claim in
                // strict mode. Strict mode persists on pooled connections, so set it explicitly.
                connection.ExecuteNonQuery("SET keeper_map_strict_mode = 0");
                connection.ExecuteNonQuery(
                    $"DELETE FROM {schema.QueueClaimKeeper} WHERE job_id = {{id:String}} AND expire_at < {{now:DateTime64(6)}}",
                    ("id", jobId), ("now", DateTime.UtcNow));
                connection.ExecuteNonQuery("SET keeper_map_strict_mode = 1");
                connection.ExecuteNonQuery(
                    $@"INSERT INTO {schema.QueueClaimKeeper} (job_id, queue, owner, enqueued_at, expire_at)
                       VALUES ({{id:String}}, {{q:String}}, {{o:String}}, {{enq:DateTime64(6)}}, {{e:DateTime64(6)}})",
                    ("id", jobId), ("q", queue), ("o", owner), ("enq", enqueuedAt), ("e", expireAt));
            });
        }
        catch (ClickHouseServerException)
        {
            // Lost the race ("Node exists") or a transient error: only a clean claim wins.
            return null;
        }

        // Reflect the fetch in job_queue so the dashboard's "Fetched" view stays accurate.
        // Best-effort: the KeeperMap claim above is the source of truth, so a failure here must
        // not discard the (successful) claim.
        try
        {
            storage.UseConnection(connection => connection.ExecuteNonQuery(
                $@"INSERT INTO {schema.JobQueue} (queue, job_id, enqueued_at, fetched_at, fetch_token, removed, ver)
                   VALUES ({{q:String}}, {{id:String}}, {{enq:DateTime64(6)}}, {{now:DateTime64(6)}}, {{o:String}}, 0, {{ver:UInt64}})",
                ("q", queue), ("id", jobId), ("enq", enqueuedAt), ("now", DateTime.UtcNow), ("o", owner), ("ver", ClickHouseVersionClock.Next())));
        }
        catch (ClickHouseServerException) { /* monitoring only */ }

        return new ClickHouseFetchedJob(storage, queue, jobId, owner, enqueuedAt);
    }

    public static void RefreshClaim(ClickHouseStorage storage, string queue, string jobId, string owner, DateTime enqueuedAt)
    {
        var expireAt = DateTime.UtcNow.Add(storage.Options.InvisibilityTimeout);
        // Non-strict upsert extends the claim's expiry (keeps the job invisible while it runs).
        storage.UseConnection(connection =>
        {
            connection.ExecuteNonQuery("SET keeper_map_strict_mode = 0");
            connection.ExecuteNonQuery(
                $@"INSERT INTO {storage.Schema.QueueClaimKeeper} (job_id, queue, owner, enqueued_at, expire_at)
                   VALUES ({{id:String}}, {{q:String}}, {{o:String}}, {{enq:DateTime64(6)}}, {{e:DateTime64(6)}})",
                ("id", jobId), ("q", queue), ("o", owner), ("enq", enqueuedAt), ("e", expireAt));
        });
    }

    public static void RemoveClaim(ClickHouseStorage storage, string queue, string jobId)
    {
        var expireAt = DateTime.UtcNow.Add(storage.Options.InvisibilityTimeout);
        storage.UseConnection(connection =>
        {
            connection.ExecuteNonQuery("SET keeper_map_strict_mode = 0");
            // Do NOT delete the claim. The candidate query reads job_queue (MergeTree) and the
            // claim (KeeperMap) from independent snapshots; deleting the claim opened a window where
            // a concurrent query saw a stale removed=0 plus the just-deleted claim and re-fetched a
            // done job. Keeping the claim present (future expiry) means the claim alone always
            // excludes the job. removed=1 is the durable marker; the expiration manager reaps the
            // expired claim later.
            connection.ExecuteNonQuery(
                $@"INSERT INTO {storage.Schema.QueueClaimKeeper} (job_id, queue, owner, enqueued_at, expire_at)
                   VALUES ({{id:String}}, {{q:String}}, '', {{now:DateTime64(6)}}, {{e:DateTime64(6)}})",
                ("id", jobId), ("q", queue), ("now", DateTime.UtcNow), ("e", expireAt));
            connection.ExecuteNonQuery(
                $@"INSERT INTO {storage.Schema.JobQueue} (queue, job_id, enqueued_at, fetched_at, fetch_token, removed, ver)
                   VALUES ({{q:String}}, {{id:String}}, {{now:DateTime64(6)}}, {{now:DateTime64(6)}}, '', 1, {{ver:UInt64}})",
                ("q", queue), ("id", jobId), ("now", DateTime.UtcNow), ("ver", ClickHouseVersionClock.Next()));
        });
    }

    public static void RequeueClaim(ClickHouseStorage storage, string jobId)
    {
        // Drop the claim; the entry (removed=0 in job_queue) becomes visible again immediately.
        storage.UseConnection(connection =>
        {
            connection.ExecuteNonQuery("SET keeper_map_strict_mode = 0");
            connection.ExecuteNonQuery(
                $"DELETE FROM {storage.Schema.QueueClaimKeeper} WHERE job_id = {{id:String}}", ("id", jobId));
        });
    }

    private static (string Clause, List<(string, object?)> Parameters) BuildInClause(string[] queues)
    {
        var builder = new StringBuilder();
        var parameters = new List<(string, object?)>(queues.Length);
        for (var i = 0; i < queues.Length; i++)
        {
            if (i > 0) builder.Append(", ");
            var name = $"q{i}";
            builder.Append('{').Append(name).Append(":String}");
            parameters.Add((name, queues[i]));
        }
        return (builder.ToString(), parameters);
    }
}
