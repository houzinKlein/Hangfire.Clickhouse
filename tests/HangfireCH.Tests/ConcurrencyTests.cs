using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using HangfireCH.Tests.Infrastructure;
using Hangfire.Common;
using Hangfire.Storage;
using Xunit;

namespace HangfireCH.Tests;

/// <summary>
/// Concurrency guarantees of the default (optimistic) path: the queue is at-least-once
/// (no job is ever lost), and append-only counters are race-free by construction. Strict
/// exactly-once / mutual-exclusion guarantees belong to the KeeperMap path (KeeperMapTests).
/// </summary>
public sealed class ConcurrencyTests : IntegrationTestBase
{
    public ConcurrencyTests(ClickHouseContainer ch) : base(ch) { }

    private static Job SampleJob() => Job.FromExpression(() => IntegrationTestBase.Sample("x"));

    [Fact]
    public void Parallel_dequeue_drains_every_job_without_loss()
    {
        using var storage = CreateStorage(o =>
        {
            o.QueuePollInterval = TimeSpan.FromMilliseconds(100);
            o.InvisibilityTimeout = TimeSpan.FromMinutes(5);
        });

        const int jobCount = 40;
        var enqueued = new HashSet<string>();
        using (var seed = Connect(storage))
        {
            for (var i = 0; i < jobCount; i++)
            {
                var id = seed.CreateExpiredJob(SampleJob(), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
                enqueued.Add(id);
                InTransaction(seed, t => t.AddToQueue("default", id));
            }
        }

        var fetched = new ConcurrentBag<string>();
        Parallel.For(0, 8, _ =>
        {
            using var connection = Connect(storage);
            while (true)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(4));
                IFetchedJob job;
                try { job = connection.FetchNextJob(new[] { "default" }, cts.Token); }
                catch (OperationCanceledException) { break; } // queue drained -> this worker stops
                fetched.Add(job.JobId);
                job.RemoveFromQueue();
                job.Dispose();
            }
        });

        // Every enqueued job was dequeued (none lost). Duplicates are tolerated (at-least-once).
        Assert.Equal(enqueued, new HashSet<string>(fetched));
    }

    [Fact]
    public void Concurrent_counter_increments_sum_correctly()
    {
        using var storage = CreateStorage();

        Parallel.For(0, 8, _ =>
        {
            using var connection = Connect(storage);
            for (var i = 0; i < 100; i++)
                InTransaction(connection, t => t.IncrementCounter("k"));
        });

        using var read = Connect(storage);
        Assert.Equal(800, read.GetCounter("k")); // append-only deltas: race-free by construction
    }

    [Fact]
    public void Distributed_lock_is_reacquirable_under_serial_contention()
    {
        using var storage = CreateStorage();

        // Acquire/release the same resource repeatedly from several threads. The best-effort
        // lock must never deadlock and must always be re-acquirable after release.
        Parallel.For(0, 6, _ =>
        {
            using var connection = Connect(storage);
            for (var i = 0; i < 5; i++)
            {
                using var handle = connection.AcquireDistributedLock("res", TimeSpan.FromSeconds(20));
                Thread.Sleep(5);
            }
        });

        using var final = Connect(storage);
        using var ok = final.AcquireDistributedLock("res", TimeSpan.FromSeconds(20));
        Assert.NotNull(ok);
    }
}
