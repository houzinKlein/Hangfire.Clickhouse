using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.ClickHouse;
using Hangfire.ClickHouse.Tests.Infrastructure;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.ClickHouse.Tests;

public sealed class StorageIntegrationTests : IntegrationTestBase
{
    public StorageIntegrationTests(ClickHouseContainer ch) : base(ch) { }

    private static Job SampleJob() => Job.FromExpression(() => IntegrationTestBase.Sample("hello"));

    [Fact]
    public void CreateExpiredJob_then_GetJobData_round_trips()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        var jobId = connection.CreateExpiredJob(SampleJob(),
            new Dictionary<string, string> { ["Culture"] = "en-US", ["RetryCount"] = "3" },
            DateTime.UtcNow, TimeSpan.FromHours(1));

        Assert.False(string.IsNullOrEmpty(jobId));

        var data = connection.GetJobData(jobId);
        Assert.NotNull(data);
        Assert.Null(data!.LoadException);
        Assert.Equal("Sample", data.Job!.Method.Name);
        Assert.Equal("hello", data.Job.Args[0]);
        Assert.Null(data.State); // no state set yet
        Assert.Equal("en-US", connection.GetJobParameter(jobId, "Culture"));
        Assert.Equal("3", connection.GetJobParameter(jobId, "RetryCount"));
        Assert.Null(connection.GetJobParameter(jobId, "Missing"));
    }

    [Fact]
    public void GetJobData_returns_null_for_unknown_job()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        Assert.Null(connection.GetJobData("nope"));
        Assert.Null(connection.GetStateData("nope"));
    }

    [Fact]
    public void SetJobParameter_is_last_writer_wins()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var jobId = connection.CreateExpiredJob(SampleJob(), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));

        connection.SetJobParameter(jobId, "p", "1");
        connection.SetJobParameter(jobId, "p", "2");

        Assert.Equal("2", connection.GetJobParameter(jobId, "p"));
    }

    [Fact]
    public void SetJobState_updates_state_and_history()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var jobId = connection.CreateExpiredJob(SampleJob(), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));

        InTransaction(connection, t => t.SetJobState(jobId,
            new TestState("Processing", "working", new Dictionary<string, string> { ["ServerId"] = "srv-1" })));

        var data = connection.GetJobData(jobId);
        Assert.Equal("Processing", data!.State);

        var state = connection.GetStateData(jobId);
        Assert.Equal("Processing", state!.Name);
        Assert.Equal("working", state.Reason);
        Assert.Equal("srv-1", state.Data["ServerId"]);
    }

    [Fact]
    public void Queue_enqueue_fetch_remove_cycle()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var jobId = connection.CreateExpiredJob(SampleJob(), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));

        InTransaction(connection, t => t.AddToQueue("default", jobId));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var fetched = connection.FetchNextJob(new[] { "default" }, cts.Token);
        Assert.Equal(jobId, fetched.JobId);

        // While fetched (invisible), another fetch finds nothing quickly.
        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        Assert.Throws<OperationCanceledException>(() => connection.FetchNextJob(new[] { "default" }, cts2.Token));

        fetched.RemoveFromQueue();
        fetched.Dispose();

        using var cts3 = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        Assert.Throws<OperationCanceledException>(() => connection.FetchNextJob(new[] { "default" }, cts3.Token));
    }

    [Fact]
    public void Queue_requeue_makes_job_fetchable_again()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var jobId = connection.CreateExpiredJob(SampleJob(), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
        InTransaction(connection, t => t.AddToQueue("default", jobId));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var fetched = connection.FetchNextJob(new[] { "default" }, cts.Token);
        fetched.Requeue();
        fetched.Dispose();

        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var again = connection.FetchNextJob(new[] { "default" }, cts2.Token);
        Assert.Equal(jobId, again.JobId);
        again.RemoveFromQueue();
        again.Dispose();
    }

    [Fact]
    public void Counters_increment_and_decrement()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        InTransaction(connection, t =>
        {
            t.IncrementCounter("c");
            t.IncrementCounter("c");
            t.IncrementCounter("c");
            t.DecrementCounter("c");
        });

        Assert.Equal(2, connection.GetCounter("c"));
    }

    [Fact]
    public void Sets_add_remove_query_and_order()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        InTransaction(connection, t =>
        {
            t.AddToSet("s", "a", 3.0);
            t.AddToSet("s", "b", 1.0);
            t.AddToSet("s", "c", 2.0);
            t.AddToSet("s", "d", 5.0);
            t.RemoveFromSet("s", "d");
        });

        Assert.Equal(new HashSet<string> { "a", "b", "c" }, connection.GetAllItemsFromSet("s"));
        Assert.Equal(3, connection.GetSetCount("s"));
        Assert.True(connection.GetSetContains("s", "a"));
        Assert.False(connection.GetSetContains("s", "d"));
        Assert.Equal("b", connection.GetFirstByLowestScoreFromSet("s", 0, 10));
        Assert.Equal(new List<string> { "b", "c" }, connection.GetFirstByLowestScoreFromSet("s", 0, 10, 2));
        Assert.Equal(new List<string> { "b", "c", "a" }, connection.GetRangeFromSet("s", 0, 10));
    }

    [Fact]
    public void Hashes_set_get_and_remove()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        connection.SetRangeInHash("h", new Dictionary<string, string> { ["f1"] = "v1", ["f2"] = "v2" });
        connection.SetRangeInHash("h", new Dictionary<string, string> { ["f2"] = "v2b" });

        Assert.Equal("v1", connection.GetValueFromHash("h", "f1"));
        Assert.Equal("v2b", connection.GetValueFromHash("h", "f2"));
        Assert.Equal(2, connection.GetHashCount("h"));

        var all = connection.GetAllEntriesFromHash("h");
        Assert.NotNull(all);
        Assert.Equal("v1", all!["f1"]);
        Assert.Equal("v2b", all["f2"]);

        InTransaction(connection, t => t.RemoveHash("h"));
        Assert.Null(connection.GetAllEntriesFromHash("h"));
        Assert.Equal(0, connection.GetHashCount("h"));
    }

    [Fact]
    public void Lists_insert_range_trim_and_remove()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        InTransaction(connection, t =>
        {
            t.InsertToList("l", "1");
            t.InsertToList("l", "2");
            t.InsertToList("l", "3"); // head = most recent
        });

        Assert.Equal(3, connection.GetListCount("l"));
        // newest first
        Assert.Equal(new List<string> { "3", "2", "1" }, connection.GetAllItemsFromList("l"));
        Assert.Equal(new List<string> { "3", "2" }, connection.GetRangeFromList("l", 0, 1));

        InTransaction(connection, t => t.RemoveFromList("l", "2"));
        Assert.Equal(new List<string> { "3", "1" }, connection.GetAllItemsFromList("l"));

        InTransaction(connection, t => t.TrimList("l", 0, 0)); // keep only head
        Assert.Equal(new List<string> { "3" }, connection.GetAllItemsFromList("l"));
    }

    [Fact]
    public void Set_expiration_makes_items_invisible()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        InTransaction(connection, t => t.AddToSet("s", "x"));
        InTransaction(connection, t => t.ExpireSet("s", TimeSpan.FromMilliseconds(-1))); // already expired

        Assert.Empty(connection.GetAllItemsFromSet("s"));

        InTransaction(connection, t => t.AddToSet("s2", "y"));
        var ttl = connection.GetSetTtl("s2");
        Assert.True(ttl.TotalSeconds < 0); // no expiration set -> negative
    }

    [Fact]
    public void Servers_announce_heartbeat_and_remove()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        connection.AnnounceServer("srv-1", new ServerContext { WorkerCount = 4, Queues = new[] { "default", "critical" } });
        connection.Heartbeat("srv-1");

        var monitoring = storage.GetMonitoringApi();
        var servers = monitoring.Servers();
        Assert.Single(servers);
        Assert.Equal("srv-1", servers[0].Name);
        Assert.Equal(4, servers[0].WorkersCount);

        connection.RemoveServer("srv-1");
        Assert.Empty(storage.GetMonitoringApi().Servers());
    }

    [Fact]
    public void RemoveTimedOutServers_removes_stale_only()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        connection.AnnounceServer("fresh", new ServerContext { WorkerCount = 1, Queues = new[] { "default" } });
        connection.AnnounceServer("stale", new ServerContext { WorkerCount = 1, Queues = new[] { "default" } });

        Thread.Sleep(1100);
        connection.Heartbeat("fresh"); // fresh heartbeat now

        var removed = connection.RemoveTimedOutServers(TimeSpan.FromSeconds(1));
        Assert.Equal(1, removed);
        Assert.Single(storage.GetMonitoringApi().Servers());
        Assert.Equal("fresh", storage.GetMonitoringApi().Servers()[0].Name);
    }

    [Fact]
    public void DistributedLock_is_exclusive_and_released()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        var first = connection.AcquireDistributedLock("res", TimeSpan.FromSeconds(2));

        using (var other = Connect(storage))
        {
            Assert.Throws<DistributedLockTimeoutException>(
                () => other.AcquireDistributedLock("res", TimeSpan.FromMilliseconds(500)));
        }

        first.Dispose();

        // Now it can be re-acquired.
        using var third = Connect(storage);
        var lockHandle = third.AcquireDistributedLock("res", TimeSpan.FromSeconds(2));
        lockHandle.Dispose();
    }

    [Fact]
    public void ExpirationManager_removes_expired_jobs()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        var liveId = connection.CreateExpiredJob(SampleJob(), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
        var deadId = connection.CreateExpiredJob(SampleJob(), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromMinutes(-1));

        new ExpirationManager(storage).Prune();

        Assert.NotNull(connection.GetJobData(liveId));
        Assert.Null(connection.GetJobData(deadId));
    }

    [Fact]
    public void CountersAggregator_folds_raw_counters()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);

        InTransaction(connection, t =>
        {
            t.IncrementCounter("stats:succeeded");
            t.IncrementCounter("stats:succeeded");
            t.IncrementCounter("stats:succeeded");
        });

        Assert.Equal(3, connection.GetCounter("stats:succeeded"));

        Thread.Sleep(1200); // pass the aggregator's 1s cutoff margin
        new CountersAggregator(storage).Aggregate();

        // value preserved after folding
        Assert.Equal(3, connection.GetCounter("stats:succeeded"));

        // raw rows consumed
        var rawRemaining = storage.UseConnection(c => c.ExecuteCount($"SELECT count() FROM {storage.Schema.Counter}"));
        Assert.Equal(0, rawRemaining);
    }
}
