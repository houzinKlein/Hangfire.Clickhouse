using System;
using System.Threading;
using Hangfire;
using HangfireCH;
using HangfireCH.Tests.Infrastructure;
using Hangfire.Storage;
using Xunit;

namespace HangfireCH.Tests;

/// <summary>
/// Drives a real <see cref="BackgroundJobServer"/> against the storage: enqueue a job and
/// confirm a worker fetches, runs, and transitions it to Succeeded. Exercises the whole SPI
/// (queue claim, transactions, default state handlers, monitoring) as Hangfire uses it.
/// </summary>
public sealed class EndToEndTests : IntegrationTestBase
{
    public EndToEndTests(ClickHouseContainer ch) : base(ch) { }

    [Fact]
    public void Enqueued_job_is_executed_and_succeeds()
    {
        using var storage = CreateStorage();
        GlobalConfiguration.Configuration.UseStorage(storage);

        var client = new BackgroundJobClient(storage);
        var jobId = client.Enqueue(() => IntegrationTestBase.Sample("e2e"));
        Assert.False(string.IsNullOrEmpty(jobId));

        var options = new BackgroundJobServerOptions
        {
            WorkerCount = 2,
            Queues = new[] { "default" },
            SchedulePollingInterval = TimeSpan.FromMilliseconds(500),
            HeartbeatInterval = TimeSpan.FromSeconds(5),
        };

        using (new BackgroundJobServer(options, storage))
        {
            Assert.True(
                WaitFor(() => StateOf(storage, jobId) == "Succeeded", TimeSpan.FromSeconds(45)),
                $"job did not succeed in time (last state = {StateOf(storage, jobId) ?? "<null>"})");

            Assert.True(
                WaitFor(() => storage.GetMonitoringApi().SucceededListCount() >= 1, TimeSpan.FromSeconds(10)),
                "succeeded job is not reflected in the dashboard statistics");
        }
    }

    private static string? StateOf(ClickHouseStorage storage, string jobId)
    {
        using var connection = (JobStorageConnection)storage.GetConnection();
        return connection.GetJobData(jobId)?.State;
    }

    private static bool WaitFor(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            Thread.Sleep(200);
        }
        return condition();
    }
}
