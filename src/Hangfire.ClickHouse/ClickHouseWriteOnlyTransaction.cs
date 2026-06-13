using System;
using System.Collections.Generic;
using Hangfire.States;
using Hangfire.Storage;
using Octonica.ClickHouseClient;

namespace Hangfire.ClickHouse;

/// <summary>
/// Buffers operations and flushes them as inserts on <see cref="Commit"/>. ClickHouse has
/// no multi-statement transactions, so commit is NOT atomic — a mid-commit failure leaves
/// partial writes. Hangfire tolerates this because operations are last-writer-wins inserts
/// and jobs are re-driven on failure (see README "Design &amp; guarantees").
/// </summary>
internal sealed class ClickHouseWriteOnlyTransaction : JobStorageTransaction
{
    private readonly ClickHouseStorage _storage;
    private readonly List<Action<ClickHouseConnection>> _commands = new();
    private ClickHouseSchema Schema => _storage.Schema;

    public ClickHouseWriteOnlyTransaction(ClickHouseStorage storage) => _storage = storage;

    private void Enqueue(Action<ClickHouseConnection> command) => _commands.Add(command);

    public override void Commit()
    {
        _storage.UseConnection(connection =>
        {
            foreach (var command in _commands)
                command(connection);
        });
    }

    public override void Dispose()
    {
        _commands.Clear();
        base.Dispose();
    }

    // ----- jobs -----

    public override void ExpireJob(string jobId, TimeSpan expireIn)
    {
        var expireAt = DateTime.UtcNow.Add(expireIn);
        Enqueue(c => c.ExecuteNonQuery(
            $@"INSERT INTO {Schema.JobExpiration} (job_id, expire_at, ver)
               VALUES ({{id:String}}, {{expire:Nullable(DateTime64(6))}}, {{ver:UInt64}})",
            ("id", jobId), ("expire", expireAt), ("ver", ClickHouseVersionClock.Next())));
    }

    public override void PersistJob(string jobId)
    {
        Enqueue(c => c.ExecuteNonQuery(
            $@"INSERT INTO {Schema.JobExpiration} (job_id, expire_at, ver)
               VALUES ({{id:String}}, NULL, {{ver:UInt64}})",
            ("id", jobId), ("ver", ClickHouseVersionClock.Next())));
    }

    public override void SetJobState(string jobId, IState state)
    {
        var name = state.Name;
        var reason = state.Reason;
        var data = ClickHouseJobSerialization.SerializeStateData(state.SerializeData());
        var now = DateTime.UtcNow;

        Enqueue(c =>
        {
            c.ExecuteNonQuery(
                $@"INSERT INTO {Schema.State} (id, job_id, name, reason, created_at, data, ver)
                   VALUES ({{sid:String}}, {{id:String}}, {{name:String}}, {{reason:Nullable(String)}}, {{created:DateTime64(6)}}, {{data:String}}, {{ver:UInt64}})",
                ("sid", Guid.NewGuid().ToString("N")), ("id", jobId), ("name", name),
                ("reason", (object?)reason), ("created", now), ("data", data), ("ver", ClickHouseVersionClock.Next()));

            c.ExecuteNonQuery(
                $@"INSERT INTO {Schema.JobState} (job_id, state_name, ver)
                   VALUES ({{id:String}}, {{name:String}}, {{ver:UInt64}})",
                ("id", jobId), ("name", name), ("ver", ClickHouseVersionClock.Next()));
        });
    }

    public override void AddJobState(string jobId, IState state)
    {
        var name = state.Name;
        var reason = state.Reason;
        var data = ClickHouseJobSerialization.SerializeStateData(state.SerializeData());
        var now = DateTime.UtcNow;

        Enqueue(c => c.ExecuteNonQuery(
            $@"INSERT INTO {Schema.State} (id, job_id, name, reason, created_at, data, ver)
               VALUES ({{sid:String}}, {{id:String}}, {{name:String}}, {{reason:Nullable(String)}}, {{created:DateTime64(6)}}, {{data:String}}, {{ver:UInt64}})",
            ("sid", Guid.NewGuid().ToString("N")), ("id", jobId), ("name", name),
            ("reason", (object?)reason), ("created", now), ("data", data), ("ver", ClickHouseVersionClock.Next())));
    }

    public override void AddToQueue(string queue, string jobId)
        => Enqueue(c => ClickHouseJobQueue.Enqueue(c, Schema, queue, jobId));

    // ----- counters -----

    public override void IncrementCounter(string key) => InsertCounter(key, +1, null);
    public override void IncrementCounter(string key, TimeSpan expireIn) => InsertCounter(key, +1, DateTime.UtcNow.Add(expireIn));
    public override void DecrementCounter(string key) => InsertCounter(key, -1, null);
    public override void DecrementCounter(string key, TimeSpan expireIn) => InsertCounter(key, -1, DateTime.UtcNow.Add(expireIn));

    private void InsertCounter(string key, long value, DateTime? expireAt)
    {
        Enqueue(c => c.ExecuteNonQuery(
            $@"INSERT INTO {Schema.Counter} (key, value, expire_at)
               VALUES ({{key:String}}, {{value:Int64}}, {{expire:Nullable(DateTime64(6))}})",
            ("key", key), ("value", value), ("expire", expireAt)));
    }

    // ----- sets -----

    public override void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

    public override void AddToSet(string key, string value, double score)
    {
        Enqueue(c => c.ExecuteNonQuery(
            $@"INSERT INTO {Schema.Set} (key, value, score, expire_at, removed, ver)
               VALUES ({{key:String}}, {{value:String}}, {{score:Float64}}, NULL, 0, {{ver:UInt64}})",
            ("key", key), ("value", value), ("score", score), ("ver", ClickHouseVersionClock.Next())));
    }

    public override void AddRangeToSet(string key, IList<string> items)
    {
        if (items is null) throw new ArgumentNullException(nameof(items));
        foreach (var item in items) AddToSet(key, item, 0.0);
    }

    public override void RemoveFromSet(string key, string value)
    {
        Enqueue(c => c.ExecuteNonQuery(
            $@"INSERT INTO {Schema.Set} (key, value, score, expire_at, removed, ver)
               VALUES ({{key:String}}, {{value:String}}, 0, NULL, 1, {{ver:UInt64}})",
            ("key", key), ("value", value), ("ver", ClickHouseVersionClock.Next())));
    }

    public override void RemoveSet(string key)
    {
        Enqueue(c =>
        {
            foreach (var value in CurrentSetValues(c, key))
                c.ExecuteNonQuery(
                    $@"INSERT INTO {Schema.Set} (key, value, score, expire_at, removed, ver)
                       VALUES ({{key:String}}, {{value:String}}, 0, NULL, 1, {{ver:UInt64}})",
                    ("key", key), ("value", value), ("ver", ClickHouseVersionClock.Next()));
        });
    }

    public override void ExpireSet(string key, TimeSpan expireIn) => ReExpireSet(key, DateTime.UtcNow.Add(expireIn));
    public override void PersistSet(string key) => ReExpireSet(key, null);

    private void ReExpireSet(string key, DateTime? expireAt)
    {
        Enqueue(c =>
        {
            using var read = c.CreateCommand(
                $@"SELECT value, argMax(score, ver) AS score FROM {Schema.Set}
                   WHERE key = {{key:String}} GROUP BY value HAVING argMax(removed, ver) = 0",
                ("key", key));
            var rows = new List<(string Value, double Score)>();
            using (var reader = read.ExecuteReader())
                while (reader.Read()) rows.Add((reader.GetStringOrEmpty(0), reader.ReadDouble(1)));

            foreach (var (value, score) in rows)
                c.ExecuteNonQuery(
                    $@"INSERT INTO {Schema.Set} (key, value, score, expire_at, removed, ver)
                       VALUES ({{key:String}}, {{value:String}}, {{score:Float64}}, {{expire:Nullable(DateTime64(6))}}, 0, {{ver:UInt64}})",
                    ("key", key), ("value", value), ("score", score), ("expire", expireAt), ("ver", ClickHouseVersionClock.Next()));
        });
    }

    // ----- hashes -----

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        if (keyValuePairs is null) throw new ArgumentNullException(nameof(keyValuePairs));
        var pairs = new List<KeyValuePair<string, string>>(keyValuePairs);

        Enqueue(c =>
        {
            foreach (var pair in pairs)
                c.ExecuteNonQuery(
                    $@"INSERT INTO {Schema.Hash} (key, field, value, expire_at, removed, ver)
                       VALUES ({{key:String}}, {{field:String}}, {{value:Nullable(String)}}, NULL, 0, {{ver:UInt64}})",
                    ("key", key), ("field", pair.Key), ("value", (object?)pair.Value), ("ver", ClickHouseVersionClock.Next()));
        });
    }

    public override void RemoveHash(string key)
    {
        Enqueue(c =>
        {
            foreach (var field in CurrentHashFields(c, key))
                c.ExecuteNonQuery(
                    $@"INSERT INTO {Schema.Hash} (key, field, value, expire_at, removed, ver)
                       VALUES ({{key:String}}, {{field:String}}, NULL, NULL, 1, {{ver:UInt64}})",
                    ("key", key), ("field", field), ("ver", ClickHouseVersionClock.Next()));
        });
    }

    public override void ExpireHash(string key, TimeSpan expireIn) => ReExpireHash(key, DateTime.UtcNow.Add(expireIn));
    public override void PersistHash(string key) => ReExpireHash(key, null);

    private void ReExpireHash(string key, DateTime? expireAt)
    {
        Enqueue(c =>
        {
            using var read = c.CreateCommand(
                $@"SELECT field, argMax(tuple(value), ver).1 AS value FROM {Schema.Hash}
                   WHERE key = {{key:String}} GROUP BY field HAVING argMax(removed, ver) = 0",
                ("key", key));
            var rows = new List<(string Field, string? Value)>();
            using (var reader = read.ExecuteReader())
                while (reader.Read()) rows.Add((reader.GetStringOrEmpty(0), reader.GetNullableString(1)));

            foreach (var (field, value) in rows)
                c.ExecuteNonQuery(
                    $@"INSERT INTO {Schema.Hash} (key, field, value, expire_at, removed, ver)
                       VALUES ({{key:String}}, {{field:String}}, {{value:Nullable(String)}}, {{expire:Nullable(DateTime64(6))}}, 0, {{ver:UInt64}})",
                    ("key", key), ("field", field), ("value", (object?)value), ("expire", expireAt), ("ver", ClickHouseVersionClock.Next()));
        });
    }

    // ----- lists -----

    public override void InsertToList(string key, string value)
    {
        var now = DateTime.UtcNow;
        Enqueue(c => c.ExecuteNonQuery(
            $@"INSERT INTO {Schema.List} (key, id, value, created_at, expire_at, removed, ver)
               VALUES ({{key:String}}, {{id:String}}, {{value:Nullable(String)}}, {{created:DateTime64(6)}}, NULL, 0, {{ver:UInt64}})",
            ("key", key), ("id", Guid.NewGuid().ToString("N")), ("value", (object?)value),
            ("created", now), ("ver", ClickHouseVersionClock.Next())));
    }

    public override void RemoveFromList(string key, string value)
    {
        Enqueue(c =>
        {
            var ids = new List<string>();
            using (var read = c.CreateCommand(
                $@"SELECT id FROM {Schema.List} WHERE key = {{key:String}}
                   GROUP BY id HAVING argMax(removed, ver) = 0 AND argMax(tuple(value), ver).1 = {{value:String}}",
                ("key", key), ("value", value)))
            using (var reader = read.ExecuteReader())
                while (reader.Read()) ids.Add(reader.GetStringOrEmpty(0));

            foreach (var id in ids)
                TombstoneListItem(c, key, id);
        });
    }

    public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
    {
        Enqueue(c =>
        {
            var ids = new List<string>();
            using (var read = c.CreateCommand(
                $@"SELECT id FROM (
                       SELECT id, min(ver) AS seq, argMax(removed, ver) AS removed,
                              argMax(tuple(expire_at), ver).1 AS e
                       FROM {Schema.List} WHERE key = {{key:String}} GROUP BY id
                   ) WHERE removed = 0 AND (e IS NULL OR e > now64(6))
                   ORDER BY seq DESC",
                ("key", key)))
            using (var reader = read.ExecuteReader())
                while (reader.Read()) ids.Add(reader.GetStringOrEmpty(0));

            for (var index = 0; index < ids.Count; index++)
            {
                if (index < keepStartingFrom || index > keepEndingAt)
                    TombstoneListItem(c, key, ids[index]);
            }
        });
    }

    public override void ExpireList(string key, TimeSpan expireIn) => ReExpireList(key, DateTime.UtcNow.Add(expireIn));
    public override void PersistList(string key) => ReExpireList(key, null);

    private void ReExpireList(string key, DateTime? expireAt)
    {
        Enqueue(c =>
        {
            var rows = new List<(string Id, string? Value, DateTime Created)>();
            using (var read = c.CreateCommand(
                $@"SELECT id, argMax(tuple(value), ver).1 AS value, argMax(created_at, ver) AS created_at
                   FROM {Schema.List} WHERE key = {{key:String}} GROUP BY id HAVING argMax(removed, ver) = 0",
                ("key", key)))
            using (var reader = read.ExecuteReader())
                while (reader.Read()) rows.Add((reader.GetStringOrEmpty(0), reader.GetNullableString(1), reader.GetUtcDateTime(2)));

            foreach (var (id, value, created) in rows)
                c.ExecuteNonQuery(
                    $@"INSERT INTO {Schema.List} (key, id, value, created_at, expire_at, removed, ver)
                       VALUES ({{key:String}}, {{id:String}}, {{value:Nullable(String)}}, {{created:DateTime64(6)}}, {{expire:Nullable(DateTime64(6))}}, 0, {{ver:UInt64}})",
                    ("key", key), ("id", id), ("value", (object?)value), ("created", created),
                    ("expire", expireAt), ("ver", ClickHouseVersionClock.Next()));
        });
    }

    private void TombstoneListItem(ClickHouseConnection c, string key, string id)
        => c.ExecuteNonQuery(
            $@"INSERT INTO {Schema.List} (key, id, value, created_at, expire_at, removed, ver)
               VALUES ({{key:String}}, {{id:String}}, NULL, now64(6), NULL, 1, {{ver:UInt64}})",
            ("key", key), ("id", id), ("ver", ClickHouseVersionClock.Next()));

    private List<string> CurrentSetValues(ClickHouseConnection c, string key)
    {
        var values = new List<string>();
        using var command = c.CreateCommand(
            $@"SELECT value FROM {Schema.Set} WHERE key = {{key:String}}
               GROUP BY value HAVING argMax(removed, ver) = 0",
            ("key", key));
        using var reader = command.ExecuteReader();
        while (reader.Read()) values.Add(reader.GetStringOrEmpty(0));
        return values;
    }

    private List<string> CurrentHashFields(ClickHouseConnection c, string key)
    {
        var fields = new List<string>();
        using var command = c.CreateCommand(
            $@"SELECT field FROM {Schema.Hash} WHERE key = {{key:String}}
               GROUP BY field HAVING argMax(removed, ver) = 0",
            ("key", key));
        using var reader = command.ExecuteReader();
        while (reader.Read()) fields.Add(reader.GetStringOrEmpty(0));
        return fields;
    }
}
