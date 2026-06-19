using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Octonica.ClickHouseClient;

namespace Hangfire.ClickHouse;

/// <summary>
/// ClickHouse implementation of <see cref="IStorageConnection"/>. Named *StorageConnection
/// to avoid colliding with Octonica's <c>ClickHouseConnection</c> (the database connection).
/// </summary>
internal sealed class ClickHouseStorageConnection : JobStorageConnection
{
    private readonly ClickHouseStorage _storage;
    private ClickHouseSchema Schema => _storage.Schema;

    public ClickHouseStorageConnection(ClickHouseStorage storage) => _storage = storage;

    public override IWriteOnlyTransaction CreateWriteTransaction()
        => new ClickHouseWriteOnlyTransaction(_storage);

    // ----- distributed locks -----

    public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
    {
        if (resource is null) throw new ArgumentNullException(nameof(resource));

        if (_storage.Options.UseKeeperMap)
            return ClickHouseKeeperMap.AcquireLock(_storage, resource, timeout);

        var owner = Guid.NewGuid().ToString("N");
        var ttlSeconds = (long)_storage.Options.DistributedLockExpiration.TotalSeconds;
        var deadline = DateTime.UtcNow + timeout;

        var sleep = 50;
        while (true)
        {
            if (_storage.UseConnection(c => TryAcquireLock(c, resource, owner, ttlSeconds)))
                return new ClickHouseDistributedLock(_storage, resource, owner);

            if (DateTime.UtcNow >= deadline)
                throw new DistributedLockTimeoutException(resource);

            Thread.Sleep(sleep);
            sleep = Math.Min(sleep * 2, 1000);
        }
    }

    private bool TryAcquireLock(ClickHouseConnection connection, string resource, string owner, long ttlSeconds)
    {
        // Is the lock free (no row, released, or expired)?
        var free = true;
        using (var command = connection.CreateCommand(
            $@"SELECT argMax(released, ver) AS released, argMax(tuple(expire_at), ver).1 AS e
               FROM {Schema.DistributedLock} WHERE resource = {{resource:String}} GROUP BY resource",
            ("resource", resource)))
        using (var reader = command.ExecuteReader())
        {
            if (reader.Read())
            {
                var released = reader.ReadInt64(0) != 0;
                var expireAt = reader.GetUtcDateTime(1);
                free = released || expireAt <= DateTime.UtcNow;
            }
        }

        if (!free) return false;

        var now = DateTime.UtcNow;
        connection.ExecuteNonQuery(
            $@"INSERT INTO {Schema.DistributedLock} (resource, owner, acquired_at, expire_at, released, ver)
               VALUES ({{resource:String}}, {{owner:String}}, {{acquired:DateTime64(6)}}, {{expire:DateTime64(6)}}, 0, {{ver:UInt64}})",
            ("resource", resource), ("owner", owner), ("acquired", now),
            ("expire", now.AddSeconds(ttlSeconds)), ("ver", ClickHouseVersionClock.Next()));

        // Read back: did we win?
        using (var command = connection.CreateCommand(
            $@"SELECT argMax(owner, ver) AS owner, argMax(released, ver) AS released
               FROM {Schema.DistributedLock} WHERE resource = {{resource:String}} GROUP BY resource",
            ("resource", resource)))
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read()) return false;
            return reader.GetStringOrEmpty(0) == owner && reader.ReadInt64(1) == 0;
        }
    }

    // ----- jobs -----

    public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
    {
        if (job is null) throw new ArgumentNullException(nameof(job));
        if (parameters is null) throw new ArgumentNullException(nameof(parameters));

        var jobId = Guid.NewGuid().ToString("N");
        var (invocationData, arguments) = ClickHouseJobSerialization.SerializeJob(job);

        _storage.UseConnection(connection =>
        {
            connection.ExecuteNonQuery(
                $@"INSERT INTO {Schema.Job} (id, invocation_data, arguments, created_at, ver)
                   VALUES ({{id:String}}, {{inv:String}}, {{args:String}}, {{created:DateTime64(6)}}, {{ver:UInt64}})",
                ("id", jobId), ("inv", invocationData), ("args", arguments),
                ("created", createdAt.ToUniversalTime()), ("ver", ClickHouseVersionClock.Next()));

            connection.ExecuteNonQuery(
                $@"INSERT INTO {Schema.JobExpiration} (job_id, expire_at, ver)
                   VALUES ({{id:String}}, {{expire:Nullable(DateTime64(6))}}, {{ver:UInt64}})",
                ("id", jobId), ("expire", createdAt.ToUniversalTime().Add(expireIn)), ("ver", ClickHouseVersionClock.Next()));

            var parameterRows = new List<object?[]>();
            foreach (var parameter in parameters)
                parameterRows.Add(new object?[] { jobId, parameter.Key, parameter.Value, ClickHouseVersionClock.Next() });

            connection.InsertRows(Schema.JobParameter,
                new[] { "job_id", "name", "value", "ver" },
                new[] { "String", "String", "Nullable(String)", "UInt64" },
                parameterRows, _storage.Options.BatchWrites);
        });

        return jobId;
    }

    public override JobData? GetJobData(string jobId)
    {
        if (jobId is null) throw new ArgumentNullException(nameof(jobId));

        return _storage.UseConnection(connection =>
        {
            string invocationData, arguments;
            DateTime createdAt;

            using (var command = connection.CreateCommand(
                $"SELECT invocation_data, arguments, created_at FROM {Schema.Job} WHERE id = {{id:String}} ORDER BY ver DESC LIMIT 1",
                ("id", jobId)))
            using (var reader = command.ExecuteReader())
            {
                if (!reader.Read()) return null;
                invocationData = reader.GetString(0);
                arguments = reader.GetString(1);
                createdAt = reader.GetUtcDateTime(2);
            }

            var stateName = connection.ExecuteScalar(
                $"SELECT argMax(state_name, ver) FROM {Schema.JobState} WHERE job_id = {{id:String}} GROUP BY job_id",
                ("id", jobId)) as string;

            var (deserialized, loadException) = ClickHouseJobSerialization.DeserializeJob(invocationData, arguments);

            return new JobData
            {
                Job = deserialized,
                State = stateName,
                CreatedAt = createdAt,
                LoadException = loadException,
            };
        });
    }

    public override StateData? GetStateData(string jobId)
    {
        if (jobId is null) throw new ArgumentNullException(nameof(jobId));

        return _storage.UseConnection(connection =>
        {
            using var command = connection.CreateCommand(
                $"SELECT name, reason, data FROM {Schema.State} WHERE job_id = {{id:String}} ORDER BY ver DESC LIMIT 1",
                ("id", jobId));
            using var reader = command.ExecuteReader();
            if (!reader.Read()) return null;

            return new StateData
            {
                Name = reader.GetStringOrEmpty(0),
                Reason = reader.GetNullableString(1),
                Data = ClickHouseJobSerialization.DeserializeStateData(reader.GetNullableString(2)),
            };
        });
    }

    public override void SetJobParameter(string id, string name, string value)
    {
        if (id is null) throw new ArgumentNullException(nameof(id));
        if (name is null) throw new ArgumentNullException(nameof(name));

        _storage.UseConnection(connection => connection.ExecuteNonQuery(
            $@"INSERT INTO {Schema.JobParameter} (job_id, name, value, ver)
               VALUES ({{id:String}}, {{name:String}}, {{value:Nullable(String)}}, {{ver:UInt64}})",
            ("id", id), ("name", name), ("value", (object?)value), ("ver", ClickHouseVersionClock.Next())));
    }

    public override string? GetJobParameter(string id, string name)
    {
        if (id is null) throw new ArgumentNullException(nameof(id));
        if (name is null) throw new ArgumentNullException(nameof(name));

        return _storage.UseConnection(connection => connection.ExecuteScalar(
            $"SELECT argMax(tuple(value), ver).1 FROM {Schema.JobParameter} WHERE job_id = {{id:String}} AND name = {{name:String}} GROUP BY job_id, name",
            ("id", id), ("name", name)) as string);
    }

    // ----- queue -----

    public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
    {
        if (queues is null || queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fetched = ClickHouseJobQueue.TryDequeue(_storage, queues);
            if (fetched is not null) return fetched;

            // Wait the poll interval or until cancellation.
            if (cancellationToken.WaitHandle.WaitOne(_storage.Options.QueuePollInterval))
                cancellationToken.ThrowIfCancellationRequested();
        }
    }

    // ----- servers -----

    public override void AnnounceServer(string serverId, ServerContext context)
    {
        if (serverId is null) throw new ArgumentNullException(nameof(serverId));
        if (context is null) throw new ArgumentNullException(nameof(context));

        var data = SerializationHelper.Serialize(new ServerData
        {
            WorkerCount = context.WorkerCount,
            Queues = context.Queues,
            StartedAt = DateTime.UtcNow,
        });

        _storage.UseConnection(connection => connection.ExecuteNonQuery(
            $@"INSERT INTO {Schema.Server} (id, data, last_heartbeat, removed, ver)
               VALUES ({{id:String}}, {{data:String}}, now64(6), 0, {{ver:UInt64}})",
            ("id", serverId), ("data", data), ("ver", ClickHouseVersionClock.Next())));
    }

    public override void Heartbeat(string serverId)
    {
        if (serverId is null) throw new ArgumentNullException(nameof(serverId));

        _storage.UseConnection(connection =>
        {
            var data = connection.ExecuteScalar(
                $"SELECT argMax(data, ver) FROM {Schema.Server} WHERE id = {{id:String}} GROUP BY id",
                ("id", serverId)) as string ?? string.Empty;

            connection.ExecuteNonQuery(
                $@"INSERT INTO {Schema.Server} (id, data, last_heartbeat, removed, ver)
                   VALUES ({{id:String}}, {{data:String}}, now64(6), 0, {{ver:UInt64}})",
                ("id", serverId), ("data", data), ("ver", ClickHouseVersionClock.Next()));
        });
    }

    public override void RemoveServer(string serverId)
    {
        if (serverId is null) throw new ArgumentNullException(nameof(serverId));

        _storage.UseConnection(connection => connection.ExecuteNonQuery(
            $@"INSERT INTO {Schema.Server} (id, data, last_heartbeat, removed, ver)
               VALUES ({{id:String}}, '', now64(6), 1, {{ver:UInt64}})",
            ("id", serverId), ("ver", ClickHouseVersionClock.Next())));
    }

    public override int RemoveTimedOutServers(TimeSpan timeOut)
    {
        if (timeOut < TimeSpan.Zero) throw new ArgumentException("Timeout must be non-negative.", nameof(timeOut));

        var threshold = DateTime.UtcNow - timeOut;

        return _storage.UseConnection(connection =>
        {
            var ids = new List<string>();
            using (var command = connection.CreateCommand(
                $@"SELECT id FROM (
                       SELECT id, argMax(last_heartbeat, ver) AS hb, argMax(removed, ver) AS removed
                       FROM {Schema.Server} GROUP BY id
                   ) WHERE removed = 0 AND hb < {{threshold:DateTime64(6)}}",
                ("threshold", threshold)))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read()) ids.Add(reader.GetString(0));
            }

            foreach (var id in ids)
            {
                connection.ExecuteNonQuery(
                    $@"INSERT INTO {Schema.Server} (id, data, last_heartbeat, removed, ver)
                       VALUES ({{id:String}}, '', now64(6), 1, {{ver:UInt64}})",
                    ("id", id), ("ver", ClickHouseVersionClock.Next()));
            }

            return ids.Count;
        });
    }

    // ----- sets -----

    public override HashSet<string> GetAllItemsFromSet(string key)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        return _storage.UseConnection(connection =>
        {
            var result = new HashSet<string>();
            using var command = connection.CreateCommand(
                $@"SELECT value FROM (
                       SELECT value, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                       FROM {Schema.Set} WHERE key = {{key:String}} GROUP BY value
                   ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))",
                ("key", key));
            using var reader = command.ExecuteReader();
            while (reader.Read()) result.Add(reader.GetStringOrEmpty(0));
            return result;
        });
    }

    public override string? GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
    {
        var list = GetFirstByLowestScoreFromSet(key, fromScore, toScore, 1);
        return list.Count > 0 ? list[0] : null;
    }

    public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));
        if (count <= 0) throw new ArgumentException("Count must be positive.", nameof(count));

        return _storage.UseConnection(connection =>
        {
            var result = new List<string>();
            using var command = connection.CreateCommand(
                $@"SELECT value FROM (
                       SELECT value, argMax(score, ver) AS score, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                       FROM {Schema.Set} WHERE key = {{key:String}} GROUP BY value
                   )
                   WHERE removed = 0 AND (e IS NULL OR e > now64(6)) AND score >= {{from:Float64}} AND score <= {{to:Float64}}
                   ORDER BY score ASC LIMIT {{count:UInt64}}",
                ("key", key), ("from", fromScore), ("to", toScore), ("count", (ulong)count));
            using var reader = command.ExecuteReader();
            while (reader.Read()) result.Add(reader.GetStringOrEmpty(0));
            return result;
        });
    }

    public override long GetSetCount(string key)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        return _storage.UseConnection(connection => connection.ExecuteCount(
            $@"SELECT count() FROM (
                   SELECT value, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                   FROM {Schema.Set} WHERE key = {{key:String}} GROUP BY value
               ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))",
            ("key", key)));
    }

    public override long GetSetCount(IEnumerable<string> keys, int limit)
    {
        if (keys is null) throw new ArgumentNullException(nameof(keys));

        var keyList = new List<string>(keys);
        if (keyList.Count == 0) return 0;

        return _storage.UseConnection(connection =>
        {
            var parameters = new List<(string, object?)>();
            var placeholders = new List<string>();
            for (var i = 0; i < keyList.Count; i++)
            {
                placeholders.Add($"{{k{i}:String}}");
                parameters.Add(($"k{i}", keyList[i]));
            }
            parameters.Add(("limit", (ulong)limit));

            using var command = connection.CreateCommand(
                $@"SELECT count() FROM (
                       SELECT key, value FROM (
                           SELECT key, value, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                           FROM {Schema.Set} WHERE key IN ({string.Join(", ", placeholders)}) GROUP BY key, value
                       ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))
                       LIMIT {{limit:UInt64}}
                   )");
            foreach (var (name, value) in parameters) command.AddParameter(name, value);
            var scalar = command.ExecuteScalar();
            return scalar is null ? 0L : Convert.ToInt64(scalar);
        });
    }

    public override bool GetSetContains(string key, string value)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));
        if (value is null) throw new ArgumentNullException(nameof(value));

        return _storage.UseConnection(connection => connection.ExecuteCount(
            $@"SELECT count() FROM (
                   SELECT value, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                   FROM {Schema.Set} WHERE key = {{key:String}} AND value = {{value:String}} GROUP BY value
               ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))",
            ("key", key), ("value", value)) > 0);
    }

    public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        var limit = Math.Max(0, endingAt - startingFrom + 1);

        return _storage.UseConnection(connection =>
        {
            var result = new List<string>();
            using var command = connection.CreateCommand(
                $@"SELECT value FROM (
                       SELECT value, argMax(score, ver) AS score, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                       FROM {Schema.Set} WHERE key = {{key:String}} GROUP BY value
                   ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))
                   ORDER BY score ASC LIMIT {{limit:UInt64}} OFFSET {{offset:UInt64}}",
                ("key", key), ("limit", (ulong)limit), ("offset", (ulong)Math.Max(0, startingFrom)));
            using var reader = command.ExecuteReader();
            while (reader.Read()) result.Add(reader.GetStringOrEmpty(0));
            return result;
        });
    }

    public override TimeSpan GetSetTtl(string key)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        return _storage.UseConnection(connection =>
        {
            var min = connection.ExecuteScalar(
                $@"SELECT min(e) FROM (
                       SELECT value, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                       FROM {Schema.Set} WHERE key = {{key:String}} GROUP BY value
                   ) WHERE removed = 0 AND e IS NOT NULL",
                ("key", key));
            return TtlFrom(min);
        });
    }

    // ----- hashes -----

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));
        if (keyValuePairs is null) throw new ArgumentNullException(nameof(keyValuePairs));

        var rows = new List<object?[]>();
        foreach (var pair in keyValuePairs)
            rows.Add(new object?[] { key, pair.Key, pair.Value, null, (byte)0, ClickHouseVersionClock.Next() });

        _storage.UseConnection(connection => connection.InsertRows(Schema.Hash,
            new[] { "key", "field", "value", "expire_at", "removed", "ver" },
            new[] { "String", "String", "Nullable(String)", "Nullable(DateTime64(6))", "UInt8", "UInt64" },
            rows, _storage.Options.BatchWrites));
    }

    public override Dictionary<string, string>? GetAllEntriesFromHash(string key)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        return _storage.UseConnection(connection =>
        {
            var result = new Dictionary<string, string>();
            using var command = connection.CreateCommand(
                $@"SELECT field, value FROM (
                       SELECT field, argMax(tuple(value), ver).1 AS value, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                       FROM {Schema.Hash} WHERE key = {{key:String}} GROUP BY field
                   ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))",
                ("key", key));
            using var reader = command.ExecuteReader();
            while (reader.Read())
                result[reader.GetStringOrEmpty(0)] = reader.GetStringOrEmpty(1);

            return result.Count == 0 ? null : result;
        });
    }

    public override string? GetValueFromHash(string key, string name)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));
        if (name is null) throw new ArgumentNullException(nameof(name));

        return _storage.UseConnection(connection => connection.ExecuteScalar(
            $@"SELECT value FROM (
                   SELECT field, argMax(tuple(value), ver).1 AS value, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                   FROM {Schema.Hash} WHERE key = {{key:String}} AND field = {{field:String}} GROUP BY field
               ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))",
            ("key", key), ("field", name)) as string);
    }

    public override long GetHashCount(string key)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        return _storage.UseConnection(connection => connection.ExecuteCount(
            $@"SELECT count() FROM (
                   SELECT field, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                   FROM {Schema.Hash} WHERE key = {{key:String}} GROUP BY field
               ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))",
            ("key", key)));
    }

    public override TimeSpan GetHashTtl(string key)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        return _storage.UseConnection(connection =>
        {
            var min = connection.ExecuteScalar(
                $@"SELECT min(e) FROM (
                       SELECT field, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                       FROM {Schema.Hash} WHERE key = {{key:String}} GROUP BY field
                   ) WHERE removed = 0 AND e IS NOT NULL",
                ("key", key));
            return TtlFrom(min);
        });
    }

    // ----- lists -----

    public override List<string> GetAllItemsFromList(string key) => GetRangeFromList(key, 0, int.MaxValue - 1);

    public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        var limit = endingAt >= int.MaxValue - 1 ? long.MaxValue : Math.Max(0, endingAt - startingFrom + 1);

        return _storage.UseConnection(connection =>
        {
            var result = new List<string>();
            // Order by the original insertion version (min ver). created_at can tie under
            // coarse OS clock resolution; min(ver) is monotonic and stable across re-inserts.
            using var command = connection.CreateCommand(
                $@"SELECT value FROM (
                       SELECT id, argMax(tuple(value), ver).1 AS value, min(ver) AS seq,
                              argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                       FROM {Schema.List} WHERE key = {{key:String}} GROUP BY id
                   ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))
                   ORDER BY seq DESC LIMIT {{limit:UInt64}} OFFSET {{offset:UInt64}}",
                ("key", key), ("limit", (ulong)limit), ("offset", (ulong)Math.Max(0, startingFrom)));
            using var reader = command.ExecuteReader();
            while (reader.Read()) result.Add(reader.GetStringOrEmpty(0));
            return result;
        });
    }

    public override long GetListCount(string key)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        return _storage.UseConnection(connection => connection.ExecuteCount(
            $@"SELECT count() FROM (
                   SELECT id, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                   FROM {Schema.List} WHERE key = {{key:String}} GROUP BY id
               ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))",
            ("key", key)));
    }

    public override TimeSpan GetListTtl(string key)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        return _storage.UseConnection(connection =>
        {
            var min = connection.ExecuteScalar(
                $@"SELECT min(e) FROM (
                       SELECT id, argMax(removed, ver) AS removed, argMax(tuple(expire_at), ver).1 AS e
                       FROM {Schema.List} WHERE key = {{key:String}} GROUP BY id
                   ) WHERE removed = 0 AND e IS NOT NULL",
                ("key", key));
            return TtlFrom(min);
        });
    }

    // ----- counters -----

    public override long GetCounter(string key)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        return _storage.UseConnection(connection =>
        {
            var raw = connection.ExecuteScalar(
                $"SELECT sum(value) FROM {Schema.Counter} WHERE key = {{key:String}}", ("key", key));
            var aggregated = connection.ExecuteScalar(
                $"SELECT sum(value) FROM {Schema.AggregatedCounter} WHERE key = {{key:String}}", ("key", key));

            return ToInt64(raw) + ToInt64(aggregated);
        });
    }

    private static long ToInt64(object? value) => value is null ? 0L : Convert.ToInt64(value);

    private static TimeSpan TtlFrom(object? minExpire)
    {
        var expire = ClickHouseExtensions.ToUtcDateTime(minExpire);
        return expire is null ? TimeSpan.FromSeconds(-1) : expire.Value - DateTime.UtcNow;
    }
}
