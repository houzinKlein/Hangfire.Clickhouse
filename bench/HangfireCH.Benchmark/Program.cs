using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using HangfireCH;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Octonica.ClickHouseClient;

// Throughput benchmark for HangfireCH. Run against a real ClickHouse (24.12+):
//
//   HANGFIRE_CLICKHOUSE="Host=localhost;Port=9000;User=default;Password=;Database=default" \
//   dotnet run -c Release --project bench/HangfireCH.Benchmark -- [jobCount]
//
// Compares BatchWrites on/off and reports ops/sec plus the resulting active MergeTree part
// counts (the metric the batching/partitioning work targets).

var baseConnection = Environment.GetEnvironmentVariable("HANGFIRE_CLICKHOUSE")
    ?? (args.Length > 0 && args[0].Contains('=') ? args[0] : "Host=localhost;Port=9000;User=default;Password=;Database=default");

var jobCount = 0;
foreach (var a in args)
    if (int.TryParse(a, out var n)) jobCount = n;
if (jobCount <= 0) jobCount = 5_000;

Console.WriteLine($"Connection: {Mask(baseConnection)}");
Console.WriteLine($"Job count : {jobCount}");
Console.WriteLine();

RunScenario(batchWrites: false);
RunScenario(batchWrites: true);

void RunScenario(bool batchWrites)
{
    var database = "bench_" + Guid.NewGuid().ToString("N");
    var options = new ClickHouseStorageOptions { DatabaseName = database, BatchWrites = batchWrites };
    using var storage = new ClickHouseStorage(baseConnection, options);

    Console.WriteLine($"=== BatchWrites = {batchWrites} (db {database}) ===");

    var jobIds = new List<string>(jobCount);

    var enqueue = Time(() =>
    {
        using var connection = (JobStorageConnection)storage.GetConnection();
        for (var i = 0; i < jobCount; i++)
        {
            var job = Job.FromExpression(() => BenchJob.Run(i));
            var id = connection.CreateExpiredJob(job,
                new Dictionary<string, string> { ["Culture"] = "en-US", ["RetryCount"] = "0" },
                DateTime.UtcNow, TimeSpan.FromHours(1));
            jobIds.Add(id);
            using var tx = (JobStorageTransaction)connection.CreateWriteTransaction();
            tx.AddToQueue("default", id);
            tx.SetJobState(id, new SimpleState("Enqueued"));
            tx.Commit();
        }
    });

    var dequeue = Time(() =>
    {
        using var connection = (JobStorageConnection)storage.GetConnection();
        for (var i = 0; i < jobCount; i++)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var fetched = connection.FetchNextJob(new[] { "default" }, cts.Token);
            fetched.RemoveFromQueue();
            fetched.Dispose();
        }
    });

    Report("enqueue (create+queue+state)", jobCount, enqueue);
    Report("dequeue (fetch+remove)", jobCount, dequeue);
    Console.WriteLine($"  active parts: {PartSummary(baseConnection, database)}");
    Console.WriteLine();
}

static TimeSpan Time(Action action)
{
    var sw = Stopwatch.StartNew();
    action();
    sw.Stop();
    return sw.Elapsed;
}

static void Report(string label, int count, TimeSpan elapsed)
    => Console.WriteLine($"  {label,-32} {count / elapsed.TotalSeconds,10:N0} ops/s  ({elapsed.TotalMilliseconds:N0} ms)");

static string PartSummary(string connectionString, string database)
{
    try
    {
        var builder = new ClickHouseConnectionStringBuilder(connectionString) { Database = "default" };
        using var connection = new ClickHouseConnection(builder.ConnectionString);
        connection.Open();
        using var command = connection.CreateCommand(
            "SELECT count() FROM system.parts WHERE database = {db:String} AND active");
        command.Parameters.AddWithValue("db", database);
        var result = command.ExecuteScalar();
        return $"{Convert.ToInt64(result):N0}";
    }
    catch (Exception ex)
    {
        return $"(unavailable: {ex.Message})";
    }
}

static string Mask(string connectionString)
    => System.Text.RegularExpressions.Regex.Replace(connectionString, "(?i)(password=)[^;]*", "$1***");

internal static class BenchJob
{
    public static void Run(int value) => _ = value;
}

internal sealed class SimpleState : IState
{
    public SimpleState(string name) => Name = name;
    public string Name { get; }
    public string? Reason => null;
    public bool IsFinal => false;
    public bool IgnoreJobLoadException => false;
    public Dictionary<string, string> SerializeData() => new() { ["EnqueuedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow) };
}
