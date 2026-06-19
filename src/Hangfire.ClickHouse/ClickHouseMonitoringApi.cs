using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Octonica.ClickHouseClient;

namespace Hangfire.ClickHouse;

/// <summary>
/// ClickHouse implementation of <see cref="IMonitoringApi"/> (dashboard data). Mirrors the
/// built-in storages: Processing/Scheduled/Succeeded/Failed/Deleted views are driven by the
/// current <c>job_state</c> (the state handlers only maintain counters, not lists), while
/// Enqueued/Fetched come from the queue and the Succeeded/Deleted statistics use counters.
/// </summary>
internal sealed class ClickHouseMonitoringApi : IMonitoringApi
{
    private readonly ClickHouseStorage _storage;
    private readonly ClickHouseStorageConnection _connection;

    public ClickHouseMonitoringApi(ClickHouseStorage storage)
    {
        _storage = storage;
        _connection = new ClickHouseStorageConnection(storage);
    }

    private ClickHouseSchema Schema => _storage.Schema;

    // ----- statistics -----

    public StatisticsDto GetStatistics()
    {
        return _storage.UseConnection(connection =>
        {
            var stateCounts = StateCounts(connection);

            return new StatisticsDto
            {
                Servers = connection.ExecuteCount(
                    $"SELECT count() FROM (SELECT id, argMax(removed, ver) AS r FROM {Schema.Server} GROUP BY id) WHERE r = 0"),
                Queues = connection.ExecuteCount(
                    $@"SELECT count(DISTINCT queue) FROM (
                           SELECT queue, job_id, argMax(removed, ver) AS r FROM {Schema.JobQueue} GROUP BY queue, job_id
                       ) WHERE r = 0"),
                Enqueued = stateCounts.GetValueOrDefault(EnqueuedState.StateName),
                Scheduled = stateCounts.GetValueOrDefault(ScheduledState.StateName),
                Processing = stateCounts.GetValueOrDefault(ProcessingState.StateName),
                Failed = stateCounts.GetValueOrDefault(FailedState.StateName),
                Awaiting = stateCounts.GetValueOrDefault(AwaitingState.StateName),
                Succeeded = _connection.GetCounter("stats:succeeded"),
                Deleted = _connection.GetCounter("stats:deleted"),
                Recurring = _connection.GetSetCount("recurring-jobs"),
            };
        });
    }

    // ----- counts -----

    public long EnqueuedCount(string queue) => QueueCount(queue, fetched: false);

    public long FetchedCount(string queue) => QueueCount(queue, fetched: true);

    public long ScheduledCount() => StateCount(ScheduledState.StateName);

    public long ProcessingCount() => StateCount(ProcessingState.StateName);

    public long FailedCount() => StateCount(FailedState.StateName);

    public long SucceededListCount() => StateCount(SucceededState.StateName);

    public long DeletedListCount() => StateCount(DeletedState.StateName);

    // ----- queues & servers -----

    public IList<QueueWithTopEnqueuedJobsDto> Queues()
    {
        var queues = _storage.UseConnection(connection =>
        {
            var names = new List<string>();
            using var command = connection.CreateCommand(
                $@"SELECT DISTINCT queue FROM (
                       SELECT queue, job_id, argMax(removed, ver) AS r FROM {Schema.JobQueue} GROUP BY queue, job_id
                   ) WHERE r = 0 ORDER BY queue");
            using var reader = command.ExecuteReader();
            while (reader.Read()) names.Add(reader.GetStringOrEmpty(0));
            return names;
        });

        return queues.Select(queue => new QueueWithTopEnqueuedJobsDto
        {
            Name = queue,
            Length = EnqueuedCount(queue),
            Fetched = FetchedCount(queue),
            FirstJobs = EnqueuedJobs(queue, 0, 5),
        }).ToList();
    }

    public IList<ServerDto> Servers()
    {
        return _storage.UseConnection(connection =>
        {
            var servers = new List<ServerDto>();
            using var command = connection.CreateCommand(
                $@"SELECT id, data, last_heartbeat FROM (
                       SELECT id, argMax(data, ver) AS data, argMax(last_heartbeat, ver) AS last_heartbeat,
                              argMax(removed, ver) AS removed
                       FROM {Schema.Server} GROUP BY id
                   ) WHERE removed = 0 ORDER BY id");
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var id = reader.GetStringOrEmpty(0);
                var data = reader.GetNullableString(1);
                var heartbeat = reader.GetNullableUtcDateTime(2);
                var parsed = string.IsNullOrEmpty(data) ? null : SerializationHelper.Deserialize<ServerData>(data);

                servers.Add(new ServerDto
                {
                    Name = id,
                    Heartbeat = heartbeat,
                    Queues = parsed?.Queues?.ToList() ?? new List<string>(),
                    StartedAt = parsed?.StartedAt ?? default,
                    WorkersCount = parsed?.WorkerCount ?? 0,
                });
            }
            return servers;
        });
    }

    // ----- queue-backed job lists -----

    public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
    {
        var ids = QueueJobIds(queue, from, perPage, fetched: false);
        var records = LoadJobs(ids);
        return ToJobList(ids, records, (_, r) => new EnqueuedJobDto
        {
            Job = r.Job,
            State = r.StateName,
            InEnqueuedState = string.Equals(r.StateName, EnqueuedState.StateName, StringComparison.OrdinalIgnoreCase),
            EnqueuedAt = ParseDate(r.StateData, "EnqueuedAt"),
        });
    }

    public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
    {
        var rows = _storage.UseConnection(connection =>
        {
            var result = new List<(string JobId, DateTime? FetchedAt)>();
            using var command = connection.CreateCommand(
                $@"SELECT job_id, f FROM (
                       SELECT job_id, argMax(tuple(fetched_at), ver).1 AS f, argMax(removed, ver) AS r, min(enqueued_at) AS enq
                       FROM {Schema.JobQueue} WHERE queue = {{queue:String}} GROUP BY job_id
                   ) WHERE r = 0 AND f IS NOT NULL
                   ORDER BY enq ASC LIMIT {{perPage:UInt64}} OFFSET {{from:UInt64}}",
                ("queue", queue), ("perPage", (ulong)perPage), ("from", (ulong)Math.Max(0, from)));
            using var reader = command.ExecuteReader();
            while (reader.Read()) result.Add((reader.GetStringOrEmpty(0), reader.GetNullableUtcDateTime(1)));
            return result;
        });

        var ids = rows.Select(r => r.JobId).ToList();
        var records = LoadJobs(ids);
        var fetchedAt = rows.ToDictionary(r => r.JobId, r => r.FetchedAt);

        return ToJobList(ids, records, (id, r) => new FetchedJobDto
        {
            Job = r.Job,
            State = r.StateName,
            FetchedAt = fetchedAt.TryGetValue(id, out var at) ? at : null,
        });
    }

    // ----- state-backed job lists -----

    public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
    {
        var ids = StateJobIds(ProcessingState.StateName, from, count);
        var records = LoadJobs(ids);
        return ToJobList(ids, records, (_, r) => new ProcessingJobDto
        {
            Job = r.Job,
            InProcessingState = string.Equals(r.StateName, ProcessingState.StateName, StringComparison.OrdinalIgnoreCase),
            ServerId = r.StateData.GetValueOrDefault("ServerId") ?? r.StateData.GetValueOrDefault("ServerName"),
            StartedAt = ParseDate(r.StateData, "StartedAt"),
        });
    }

    public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
    {
        var ids = StateJobIds(ScheduledState.StateName, from, count);
        var records = LoadJobs(ids);
        return ToJobList(ids, records, (_, r) => new ScheduledJobDto
        {
            Job = r.Job,
            InScheduledState = string.Equals(r.StateName, ScheduledState.StateName, StringComparison.OrdinalIgnoreCase),
            EnqueueAt = ParseDate(r.StateData, "EnqueueAt") ?? default,
            ScheduledAt = ParseDate(r.StateData, "ScheduledAt"),
        });
    }

    public JobList<SucceededJobDto> SucceededJobs(int from, int count)
    {
        var ids = StateJobIds(SucceededState.StateName, from, count);
        var records = LoadJobs(ids);
        return ToJobList(ids, records, (_, r) => new SucceededJobDto
        {
            Job = r.Job,
            InSucceededState = string.Equals(r.StateName, SucceededState.StateName, StringComparison.OrdinalIgnoreCase),
            Result = r.StateData.GetValueOrDefault("Result"),
            TotalDuration = SumDurations(r.StateData),
            SucceededAt = ParseDate(r.StateData, "SucceededAt"),
        });
    }

    public JobList<FailedJobDto> FailedJobs(int from, int count)
    {
        var ids = StateJobIds(FailedState.StateName, from, count);
        var records = LoadJobs(ids);
        return ToJobList(ids, records, (_, r) => new FailedJobDto
        {
            Job = r.Job,
            InFailedState = string.Equals(r.StateName, FailedState.StateName, StringComparison.OrdinalIgnoreCase),
            Reason = r.StateReason,
            ExceptionDetails = r.StateData.GetValueOrDefault("ExceptionDetails"),
            ExceptionMessage = r.StateData.GetValueOrDefault("ExceptionMessage"),
            ExceptionType = r.StateData.GetValueOrDefault("ExceptionType"),
            FailedAt = ParseDate(r.StateData, "FailedAt"),
        });
    }

    public JobList<DeletedJobDto> DeletedJobs(int from, int count)
    {
        var ids = StateJobIds(DeletedState.StateName, from, count);
        var records = LoadJobs(ids);
        return ToJobList(ids, records, (_, r) => new DeletedJobDto
        {
            Job = r.Job,
            InDeletedState = string.Equals(r.StateName, DeletedState.StateName, StringComparison.OrdinalIgnoreCase),
            DeletedAt = ParseDate(r.StateData, "DeletedAt"),
        });
    }

    public JobDetailsDto? JobDetails(string jobId)
    {
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

            var expireAt = connection.ExecuteScalar(
                $"SELECT argMax(tuple(expire_at), ver).1 FROM {Schema.JobExpiration} WHERE job_id = {{id:String}} GROUP BY job_id",
                ("id", jobId));

            var properties = new Dictionary<string, string>();
            using (var command = connection.CreateCommand(
                $@"SELECT name, value FROM (
                       SELECT name, argMax(tuple(value), ver).1 AS value FROM {Schema.JobParameter}
                       WHERE job_id = {{id:String}} GROUP BY name
                   )",
                ("id", jobId)))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                    properties[reader.GetStringOrEmpty(0)] = reader.GetStringOrEmpty(1);
            }

            var history = new List<StateHistoryDto>();
            using (var command = connection.CreateCommand(
                $"SELECT name, reason, created_at, data FROM {Schema.State} WHERE job_id = {{id:String}} ORDER BY ver DESC",
                ("id", jobId)))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    history.Add(new StateHistoryDto
                    {
                        StateName = reader.GetStringOrEmpty(0),
                        Reason = reader.GetNullableString(1),
                        CreatedAt = reader.GetUtcDateTime(2),
                        Data = ClickHouseJobSerialization.DeserializeStateData(reader.GetNullableString(3)),
                    });
                }
            }

            var (job, _) = ClickHouseJobSerialization.DeserializeJob(invocationData, arguments);

            return new JobDetailsDto
            {
                Job = job,
                CreatedAt = createdAt,
                ExpireAt = ClickHouseExtensions.ToUtcDateTime(expireAt),
                Properties = properties,
                History = history,
            };
        });
    }

    // ----- timeline stats (counter-backed) -----

    public IDictionary<DateTime, long> SucceededByDatesCount() => DateCounts("stats:succeeded", days: 7);

    public IDictionary<DateTime, long> FailedByDatesCount() => DateCounts("stats:failed", days: 7);

    public IDictionary<DateTime, long> HourlySucceededJobs() => HourCounts("stats:succeeded");

    public IDictionary<DateTime, long> HourlyFailedJobs() => HourCounts("stats:failed");

    private IDictionary<DateTime, long> DateCounts(string prefix, int days)
    {
        var keys = new Dictionary<DateTime, string>();
        for (var i = 0; i < days; i++)
        {
            var date = DateTime.UtcNow.Date.AddDays(-i);
            keys[date] = $"{prefix}:{date:yyyy-MM-dd}";
        }
        return ReadCounters(keys);
    }

    private IDictionary<DateTime, long> HourCounts(string prefix)
    {
        var keys = new Dictionary<DateTime, string>();
        var now = DateTime.UtcNow;
        var hour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Utc);
        for (var i = 0; i < 24; i++)
        {
            var slot = hour.AddHours(-i);
            keys[slot] = $"{prefix}:{slot:yyyy-MM-dd-HH}";
        }
        return ReadCounters(keys);
    }

    private IDictionary<DateTime, long> ReadCounters(Dictionary<DateTime, string> keys)
    {
        var result = new Dictionary<DateTime, long>();
        foreach (var pair in keys)
            result[pair.Key] = _connection.GetCounter(pair.Value);
        return result;
    }

    // ----- helpers -----

    // FINAL collapses the ReplacingMergeTree to the current row per job_id at query time —
    // cleaner and faster on large tables than a nested argMax/GROUP BY job_id. job_state is
    // unpartitioned, so FINAL is straightforward.
    private long StateCount(string stateName)
    {
        return _storage.UseConnection(connection => connection.ExecuteCount(
            $"SELECT count() FROM {Schema.JobState} FINAL WHERE state_name = {{state:String}}",
            ("state", stateName)));
    }

    private Dictionary<string, long> StateCounts(ClickHouseConnection connection)
    {
        var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        using var command = connection.CreateCommand(
            $"SELECT state_name, count() FROM {Schema.JobState} FINAL GROUP BY state_name");
        using var reader = command.ExecuteReader();
        while (reader.Read()) result[reader.GetStringOrEmpty(0)] = reader.ReadInt64(1);
        return result;
    }

    private List<string> StateJobIds(string stateName, int from, int count)
    {
        return _storage.UseConnection(connection =>
        {
            var ids = new List<string>();
            using var command = connection.CreateCommand(
                $@"SELECT job_id FROM {Schema.JobState} FINAL
                   WHERE state_name = {{state:String}}
                   ORDER BY ver DESC LIMIT {{count:UInt64}} OFFSET {{from:UInt64}}",
                ("state", stateName), ("count", (ulong)count), ("from", (ulong)Math.Max(0, from)));
            using var reader = command.ExecuteReader();
            while (reader.Read()) ids.Add(reader.GetStringOrEmpty(0));
            return ids;
        });
    }

    private long QueueCount(string queue, bool fetched)
    {
        var predicate = fetched ? "f IS NOT NULL" : "f IS NULL";
        return _storage.UseConnection(connection => connection.ExecuteCount(
            $@"SELECT count() FROM (
                   SELECT job_id, argMax(tuple(fetched_at), ver).1 AS f, argMax(removed, ver) AS r
                   FROM {Schema.JobQueue} WHERE queue = {{queue:String}} GROUP BY job_id
               ) WHERE r = 0 AND {predicate}",
            ("queue", queue)));
    }

    private List<string> QueueJobIds(string queue, int from, int perPage, bool fetched)
    {
        var predicate = fetched ? "f IS NOT NULL" : "f IS NULL";
        return _storage.UseConnection(connection =>
        {
            var ids = new List<string>();
            using var command = connection.CreateCommand(
                $@"SELECT job_id FROM (
                       SELECT job_id, argMax(tuple(fetched_at), ver).1 AS f, argMax(removed, ver) AS r, min(enqueued_at) AS enq
                       FROM {Schema.JobQueue} WHERE queue = {{queue:String}} GROUP BY job_id
                   ) WHERE r = 0 AND {predicate}
                   ORDER BY enq ASC LIMIT {{perPage:UInt64}} OFFSET {{from:UInt64}}",
                ("queue", queue), ("perPage", (ulong)perPage), ("from", (ulong)Math.Max(0, from)));
            using var reader = command.ExecuteReader();
            while (reader.Read()) ids.Add(reader.GetStringOrEmpty(0));
            return ids;
        });
    }

    private Dictionary<string, JobRecord> LoadJobs(IReadOnlyList<string> jobIds)
    {
        var result = new Dictionary<string, JobRecord>();
        if (jobIds.Count == 0) return result;

        _storage.UseConnection(connection =>
        {
            var (clause, parameters) = BuildIn(jobIds);

            using (var command = connection.CreateCommand(
                $"SELECT id, invocation_data, arguments, created_at FROM {Schema.Job} WHERE id IN ({clause})"))
            {
                foreach (var (name, value) in parameters) command.AddParameter(name, value);
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var id = reader.GetStringOrEmpty(0);
                    var (job, loadException) = ClickHouseJobSerialization.DeserializeJob(reader.GetString(1), reader.GetString(2));
                    result[id] = new JobRecord
                    {
                        Job = job,
                        LoadException = loadException,
                        CreatedAt = reader.GetUtcDateTime(3),
                        StateData = new Dictionary<string, string>(),
                    };
                }
            }

            using (var command = connection.CreateCommand(
                $@"SELECT job_id, argMax(name, ver) AS name, argMax(tuple(reason), ver).1 AS reason, argMax(data, ver) AS data
                   FROM {Schema.State} WHERE job_id IN ({clause}) GROUP BY job_id"))
            {
                foreach (var (name, value) in parameters) command.AddParameter(name, value);
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var id = reader.GetStringOrEmpty(0);
                    if (!result.TryGetValue(id, out var record)) continue;
                    record.StateName = reader.GetStringOrEmpty(1);
                    record.StateReason = reader.GetNullableString(2);
                    record.StateData = ClickHouseJobSerialization.DeserializeStateData(reader.GetNullableString(3));
                }
            }
        });

        return result;
    }

    private static JobList<TDto> ToJobList<TDto>(IReadOnlyList<string> orderedIds, Dictionary<string, JobRecord> records,
        Func<string, JobRecord, TDto> selector)
    {
        var items = new List<KeyValuePair<string, TDto>>();
        foreach (var id in orderedIds)
        {
            var record = records.TryGetValue(id, out var r) ? r : new JobRecord { StateData = new Dictionary<string, string>() };
            items.Add(new KeyValuePair<string, TDto>(id, selector(id, record)));
        }
        return new JobList<TDto>(items);
    }

    private static (string Clause, List<(string, object?)> Parameters) BuildIn(IReadOnlyList<string> values)
    {
        var placeholders = new List<string>(values.Count);
        var parameters = new List<(string, object?)>(values.Count);
        for (var i = 0; i < values.Count; i++)
        {
            placeholders.Add($"{{j{i}:String}}");
            parameters.Add(($"j{i}", values[i]));
        }
        return (string.Join(", ", placeholders), parameters);
    }

    private static DateTime? ParseDate(IDictionary<string, string> data, string key)
    {
        if (!data.TryGetValue(key, out var value) || string.IsNullOrEmpty(value)) return null;
        try { return JobHelper.DeserializeNullableDateTime(value); }
        catch { return null; }
    }

    private static long? SumDurations(IDictionary<string, string> data)
    {
        long total = 0;
        var any = false;
        foreach (var key in new[] { "PerformanceDuration", "Latency" })
        {
            if (data.TryGetValue(key, out var value) && long.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
            {
                total += parsed;
                any = true;
            }
        }
        return any ? total : null;
    }

    private sealed class JobRecord
    {
        public Job? Job { get; set; }
        public JobLoadException? LoadException { get; set; }
        public DateTime CreatedAt { get; set; }
        public string StateName { get; set; } = string.Empty;
        public string? StateReason { get; set; }
        public Dictionary<string, string> StateData { get; set; } = new();
    }
}
