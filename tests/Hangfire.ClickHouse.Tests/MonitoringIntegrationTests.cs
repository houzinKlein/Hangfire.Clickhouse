using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.ClickHouse;
using Hangfire.ClickHouse.Tests.Infrastructure;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.ClickHouse.Tests;

// Hangfire 1.8 dashboard reads are STATE-based (current job_state) for
// Processing/Scheduled/Succeeded/Failed/Deleted, queue-based for Enqueued/Fetched, and
// counter-based for the Succeeded/Deleted statistics totals.
public sealed class MonitoringIntegrationTests : IntegrationTestBase
{
    public MonitoringIntegrationTests(ClickHouseContainer ch) : base(ch) { }

    private static Job SampleJob() => Job.FromExpression(() => IntegrationTestBase.Sample("hello"));

    private static string NewJob(JobStorageConnection c)
        => c.CreateExpiredJob(SampleJob(), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));

    [Fact]
    public void EnqueuedJobs_and_count()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var api = storage.GetMonitoringApi();

        var jobId = NewJob(connection);
        InTransaction(connection, t =>
        {
            t.AddToQueue("default", jobId);
            t.SetJobState(jobId, new TestState("Enqueued", data: new Dictionary<string, string>
            {
                ["EnqueuedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
                ["Queue"] = "default",
            }));
        });

        Assert.Equal(1, api.EnqueuedCount("default"));
        Assert.Equal(0, api.FetchedCount("default"));

        var jobs = api.EnqueuedJobs("default", 0, 10);
        Assert.Single(jobs);
        Assert.Equal(jobId, jobs[0].Key);
        Assert.True(jobs[0].Value.InEnqueuedState);
        Assert.Equal("Sample", jobs[0].Value.Job!.Method.Name);
    }

    [Fact]
    public void ProcessingJobs_by_state()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var api = storage.GetMonitoringApi();

        var jobId = NewJob(connection);
        InTransaction(connection, t => t.SetJobState(jobId, new TestState("Processing", data: new Dictionary<string, string>
        {
            ["ServerId"] = "srv-7",
            ["StartedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
        })));

        Assert.Equal(1, api.ProcessingCount());
        var jobs = api.ProcessingJobs(0, 10);
        Assert.Single(jobs);
        Assert.True(jobs[0].Value.InProcessingState);
        Assert.Equal("srv-7", jobs[0].Value.ServerId);
        Assert.NotNull(jobs[0].Value.StartedAt);
    }

    [Fact]
    public void ScheduledJobs_by_state()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var api = storage.GetMonitoringApi();

        var jobId = NewJob(connection);
        var enqueueAt = DateTime.UtcNow.AddMinutes(5);
        InTransaction(connection, t => t.SetJobState(jobId, new TestState("Scheduled", data: new Dictionary<string, string>
        {
            ["EnqueueAt"] = JobHelper.SerializeDateTime(enqueueAt),
            ["ScheduledAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
        })));

        Assert.Equal(1, api.ScheduledCount());
        var jobs = api.ScheduledJobs(0, 10);
        Assert.Single(jobs);
        Assert.True(jobs[0].Value.InScheduledState);
    }

    [Fact]
    public void SucceededJobs_by_state()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var api = storage.GetMonitoringApi();

        var jobId = NewJob(connection);
        InTransaction(connection, t => t.SetJobState(jobId, new TestState("Succeeded", data: new Dictionary<string, string>
        {
            ["SucceededAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
            ["PerformanceDuration"] = "120",
            ["Latency"] = "30",
            ["Result"] = "42",
        })));

        Assert.Equal(1, api.SucceededListCount());
        var jobs = api.SucceededJobs(0, 10);
        Assert.Single(jobs);
        Assert.True(jobs[0].Value.InSucceededState);
        Assert.Equal(150, jobs[0].Value.TotalDuration);
        Assert.Equal("42", jobs[0].Value.Result);
    }

    [Fact]
    public void FailedJobs_by_state()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var api = storage.GetMonitoringApi();

        var jobId = NewJob(connection);
        InTransaction(connection, t => t.SetJobState(jobId, new TestState("Failed", "boom", new Dictionary<string, string>
        {
            ["FailedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
            ["ExceptionType"] = "System.InvalidOperationException",
            ["ExceptionMessage"] = "boom",
            ["ExceptionDetails"] = "stack...",
        })));

        Assert.Equal(1, api.FailedCount());
        var jobs = api.FailedJobs(0, 10);
        Assert.Single(jobs);
        Assert.True(jobs[0].Value.InFailedState);
        Assert.Equal("boom", jobs[0].Value.Reason);
        Assert.Equal("System.InvalidOperationException", jobs[0].Value.ExceptionType);
    }

    [Fact]
    public void DeletedJobs_by_state()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var api = storage.GetMonitoringApi();

        var jobId = NewJob(connection);
        InTransaction(connection, t => t.SetJobState(jobId, new TestState("Deleted", data: new Dictionary<string, string>
        {
            ["DeletedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
        })));

        Assert.Equal(1, api.DeletedListCount());
        var jobs = api.DeletedJobs(0, 10);
        Assert.Single(jobs);
        Assert.True(jobs[0].Value.InDeletedState);
    }

    [Fact]
    public void GetStatistics_aggregates_everything()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var api = storage.GetMonitoringApi();

        connection.AnnounceServer("srv", new ServerContext { WorkerCount = 2, Queues = new[] { "default" } });

        var enqueued = NewJob(connection);
        var processing = NewJob(connection);
        var scheduled = NewJob(connection);
        var failed = NewJob(connection);

        InTransaction(connection, t =>
        {
            t.AddToQueue("default", enqueued);
            t.SetJobState(enqueued, new TestState("Enqueued"));
            t.SetJobState(processing, new TestState("Processing"));
            t.SetJobState(scheduled, new TestState("Scheduled"));
            t.SetJobState(failed, new TestState("Failed"));
            t.AddToSet("recurring-jobs", "my-recurring");
            t.IncrementCounter("stats:succeeded");
            t.IncrementCounter("stats:succeeded");
            t.IncrementCounter("stats:deleted");
        });

        var stats = api.GetStatistics();
        Assert.Equal(1, stats.Servers);
        Assert.Equal(1, stats.Enqueued);
        Assert.Equal(1, stats.Processing);
        Assert.Equal(1, stats.Scheduled);
        Assert.Equal(1, stats.Failed);
        Assert.Equal(2, stats.Succeeded);
        Assert.Equal(1, stats.Deleted);
        Assert.Equal(1, stats.Recurring);
        Assert.Equal(1, stats.Queues);
    }

    [Fact]
    public void Queues_lists_queue_summaries()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var api = storage.GetMonitoringApi();

        InTransaction(connection, t =>
        {
            t.AddToQueue("default", NewJob(connection));
            t.AddToQueue("default", NewJob(connection));
            t.AddToQueue("critical", NewJob(connection));
        });

        var queues = api.Queues().OrderBy(q => q.Name).ToList();
        Assert.Equal(2, queues.Count);
        Assert.Equal("critical", queues[0].Name);
        Assert.Equal(1, queues[0].Length);
        Assert.Equal("default", queues[1].Name);
        Assert.Equal(2, queues[1].Length);
    }

    [Fact]
    public void JobDetails_returns_job_parameters_and_history()
    {
        using var storage = CreateStorage();
        using var connection = Connect(storage);
        var api = storage.GetMonitoringApi();

        var jobId = connection.CreateExpiredJob(SampleJob(),
            new Dictionary<string, string> { ["RetryCount"] = "1" }, DateTime.UtcNow, TimeSpan.FromHours(1));

        InTransaction(connection, t =>
        {
            t.SetJobState(jobId, new TestState("Enqueued"));
            t.SetJobState(jobId, new TestState("Processing"));
        });

        var details = api.JobDetails(jobId);
        Assert.NotNull(details);
        Assert.Equal("Sample", details!.Job!.Method.Name);
        Assert.Equal("1", details.Properties["RetryCount"]);
        Assert.True(details.History.Count >= 2);
        Assert.Equal("Processing", details.History[0].StateName); // newest first
        Assert.NotNull(details.ExpireAt);
    }
}
