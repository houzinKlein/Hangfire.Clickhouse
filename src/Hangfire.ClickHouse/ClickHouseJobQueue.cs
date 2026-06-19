using System;
using System.Collections.Generic;
using System.Text;
using Octonica.ClickHouseClient;

namespace Hangfire.ClickHouse;

/// <summary>
/// Polling job queue. A queue entry lives as the latest version of a
/// <c>(queue, job_id)</c> row in the <c>job_queue</c> ReplacingMergeTree. Dequeue is an
/// optimistic claim: pick the oldest visible entry, insert a claim row stamped with a
/// unique token, then read back the winning token. Combined with the invisibility timeout
/// this is at-least-once (see README "Design &amp; guarantees").
/// </summary>
internal static class ClickHouseJobQueue
{
    public static void Enqueue(ClickHouseConnection connection, ClickHouseSchema schema, string queue, string jobId)
    {
        connection.ExecuteNonQuery(
            $@"INSERT INTO {schema.JobQueue} (queue, job_id, enqueued_at, fetched_at, fetch_token, removed, ver)
               VALUES ({{queue:String}}, {{job_id:String}}, {{enq:DateTime64(6)}}, NULL, '', 0, {{ver:UInt64}})",
            ("queue", queue), ("job_id", jobId), ("enq", DateTime.UtcNow), ("ver", ClickHouseVersionClock.Next()));
    }

    public static ClickHouseFetchedJob? TryDequeue(ClickHouseStorage storage, string[] queues)
    {
        if (queues.Length == 0) return null;

        if (storage.Options.UseKeeperMap)
            return ClickHouseKeeperMap.TryClaim(storage, queues);

        var token = Guid.NewGuid().ToString("N");
        var threshold = DateTime.UtcNow - storage.Options.InvisibilityTimeout;

        return storage.UseConnection(connection =>
        {
            var (inClause, inParameters) = BuildInClause(queues);

            string? queue = null, jobId = null;
            DateTime enqueuedAt = default;

            using (var command = connection.CreateCommand(
                $@"SELECT queue, job_id, enqueued_at FROM (
                       SELECT queue, job_id,
                              argMax(tuple(fetched_at), ver).1 AS fetched_at,
                              argMax(removed, ver) AS removed,
                              min(enqueued_at) AS enqueued_at
                       FROM {storage.Schema.JobQueue}
                       WHERE queue IN ({inClause})
                       GROUP BY queue, job_id
                   )
                   WHERE removed = 0 AND (fetched_at IS NULL OR fetched_at < {{threshold:DateTime64(6)}})
                   ORDER BY enqueued_at ASC
                   LIMIT 1"))
            {
                foreach (var (name, value) in inParameters) command.AddParameter(name, value);
                command.AddParameter("threshold", threshold);

                using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    queue = reader.GetString(0);
                    jobId = reader.GetString(1);
                    enqueuedAt = reader.GetUtcDateTime(2);
                }
            }

            if (queue is null || jobId is null) return null;

            // Claim it.
            connection.ExecuteNonQuery(
                $@"INSERT INTO {storage.Schema.JobQueue} (queue, job_id, enqueued_at, fetched_at, fetch_token, removed, ver)
                   VALUES ({{queue:String}}, {{job_id:String}}, {{enq:DateTime64(6)}}, {{fetched:DateTime64(6)}}, {{token:String}}, 0, {{ver:UInt64}})",
                ("queue", queue), ("job_id", jobId), ("enq", enqueuedAt),
                ("fetched", DateTime.UtcNow), ("token", token), ("ver", ClickHouseVersionClock.Next()));

            // Read back the winner.
            var winner = connection.ExecuteScalar(
                $"SELECT argMax(fetch_token, ver) FROM {storage.Schema.JobQueue} WHERE queue = {{queue:String}} AND job_id = {{job_id:String}} GROUP BY queue, job_id",
                ("queue", queue), ("job_id", jobId)) as string;

            return winner == token
                ? new ClickHouseFetchedJob(storage, queue, jobId, token, enqueuedAt)
                : null;
        });
    }

    public static void Refresh(ClickHouseStorage storage, string queue, string jobId, string token, DateTime enqueuedAt)
    {
        if (storage.Options.UseKeeperMap) { ClickHouseKeeperMap.RefreshClaim(storage, queue, jobId, token, enqueuedAt); return; }
        Write(storage, queue, jobId, enqueuedAt, fetchedAt: DateTime.UtcNow, token: token, removed: 0);
    }

    public static void Remove(ClickHouseStorage storage, string queue, string jobId, string token, DateTime enqueuedAt)
    {
        if (storage.Options.UseKeeperMap) { ClickHouseKeeperMap.RemoveClaim(storage, queue, jobId); return; }
        Write(storage, queue, jobId, enqueuedAt, fetchedAt: DateTime.UtcNow, token: token, removed: 1);
    }

    public static void Requeue(ClickHouseStorage storage, string queue, string jobId, DateTime enqueuedAt)
    {
        if (storage.Options.UseKeeperMap) { ClickHouseKeeperMap.RequeueClaim(storage, jobId); return; }
        Write(storage, queue, jobId, enqueuedAt, fetchedAt: null, token: string.Empty, removed: 0);
    }

    private static void Write(ClickHouseStorage storage, string queue, string jobId, DateTime enqueuedAt,
        DateTime? fetchedAt, string token, byte removed)
    {
        storage.UseConnection(connection => connection.ExecuteNonQuery(
            $@"INSERT INTO {storage.Schema.JobQueue} (queue, job_id, enqueued_at, fetched_at, fetch_token, removed, ver)
               VALUES ({{queue:String}}, {{job_id:String}}, {{enq:DateTime64(6)}}, {{fetched:Nullable(DateTime64(6))}}, {{token:String}}, {{removed:UInt8}}, {{ver:UInt64}})",
            ("queue", queue), ("job_id", jobId), ("enq", enqueuedAt), ("fetched", fetchedAt),
            ("token", token), ("removed", removed), ("ver", ClickHouseVersionClock.Next())));
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
