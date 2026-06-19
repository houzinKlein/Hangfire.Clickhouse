using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.ClickHouse;
using Hangfire.ClickHouse.Tests.Infrastructure;
using Hangfire.Common;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.ClickHouse.Tests;

/// <summary>
/// The opt-in KeeperMap path: true mutual exclusion and exactly-once dequeue (stronger than
/// the optimistic path's best-effort / at-least-once guarantees). Runs against a Keeper-enabled
/// ClickHouse.
/// </summary>
[Collection("clickhouse-keeper")]
public sealed class KeeperMapTests
{
    private readonly KeeperClickHouseContainer _ch;

    public KeeperMapTests(KeeperClickHouseContainer ch) => _ch = ch;

    private static Job SampleJob() => Job.FromExpression(() => IntegrationTestBase.Sample("x"));

    private ClickHouseStorage CreateStorage() => new(_ch.ConnectionString("default"), new ClickHouseStorageOptions
    {
        DatabaseName = "k_" + Guid.NewGuid().ToString("N"),
        UseKeeperMap = true, // server prepends its keeper_map_path_prefix, so the default "/" prefix is correct
        QueuePollInterval = TimeSpan.FromMilliseconds(100),
        InvisibilityTimeout = TimeSpan.FromMinutes(5),
    });

    [Fact]
    public void Lock_is_exclusive_and_released()
    {
        using var storage = CreateStorage();
        using var connection = storage.GetConnection();

        var first = connection.AcquireDistributedLock("res", TimeSpan.FromSeconds(2));

        using (var other = storage.GetConnection())
            Assert.Throws<DistributedLockTimeoutException>(
                () => other.AcquireDistributedLock("res", TimeSpan.FromMilliseconds(500)));

        first.Dispose();

        using var third = storage.GetConnection();
        third.AcquireDistributedLock("res", TimeSpan.FromSeconds(2)).Dispose();
    }

    [Fact]
    public void Lock_serializes_concurrent_critical_sections()
    {
        using var storage = CreateStorage();

        var counter = 0; // guarded only by the distributed lock; correct iff exclusion is real
        Parallel.For(0, 6, _ =>
        {
            using var connection = storage.GetConnection();
            for (var i = 0; i < 20; i++)
            {
                using var handle = connection.AcquireDistributedLock("counter", TimeSpan.FromSeconds(60));
                counter++;
            }
        });

        Assert.Equal(120, counter);
    }

    [Fact]
    public void Queue_claim_dequeues_each_job_exactly_once()
    {
        using var storage = CreateStorage();

        const int jobCount = 30;
        var enqueued = new HashSet<string>();
        using (var seed = storage.GetConnection())
        {
            for (var i = 0; i < jobCount; i++)
            {
                var id = seed.CreateExpiredJob(SampleJob(), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
                enqueued.Add(id);
                using var tx = seed.CreateWriteTransaction();
                tx.AddToQueue("default", id);
                tx.Commit();
            }
        }

        var fetched = new ConcurrentBag<string>();
        Parallel.For(0, 6, _ =>
        {
            using var connection = storage.GetConnection();
            while (true)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(4));
                IFetchedJob job;
                try { job = connection.FetchNextJob(new[] { "default" }, cts.Token); }
                catch (OperationCanceledException) { break; }
                fetched.Add(job.JobId);
                job.RemoveFromQueue();
                job.Dispose();
            }
        });

        var all = new List<string>(fetched);
        var distinct = new HashSet<string>(all);
        Assert.True(distinct.SetEquals(enqueued),
            $"set mismatch: fetched distinct {distinct.Count}, enqueued {enqueued.Count}, missing {enqueued.Count - distinct.Count}");
        Assert.True(all.Count == jobCount,
            $"expected exactly {jobCount} fetches (exactly-once), got {all.Count} (distinct {distinct.Count})");
    }
}
