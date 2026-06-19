using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using HangfireCH;
using Xunit;

namespace HangfireCH.Tests;

/// <summary>Pure unit tests that need no ClickHouse server.</summary>
public sealed class UnitTests
{
    [Fact]
    public void Options_have_sensible_defaults()
    {
        var options = new ClickHouseStorageOptions();

        Assert.True(options.PrepareSchemaIfNecessary);
        Assert.Equal(string.Empty, options.TablePrefix);
        Assert.Null(options.DatabaseName);
        Assert.Equal(TimeSpan.FromSeconds(15), options.QueuePollInterval);
        Assert.Equal(TimeSpan.FromMinutes(30), options.InvisibilityTimeout);
        Assert.Equal(TimeSpan.FromMinutes(30), options.JobExpirationCheckInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), options.CountersAggregateInterval);
        Assert.Equal(TimeSpan.FromMinutes(30), options.DistributedLockExpiration);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Options_reject_non_positive_intervals(int seconds)
    {
        var options = new ClickHouseStorageOptions();
        var value = TimeSpan.FromSeconds(seconds);

        Assert.Throws<ArgumentException>(() => options.QueuePollInterval = value);
        Assert.Throws<ArgumentException>(() => options.InvisibilityTimeout = value);
        Assert.Throws<ArgumentException>(() => options.JobExpirationCheckInterval = value);
        Assert.Throws<ArgumentException>(() => options.CountersAggregateInterval = value);
        Assert.Throws<ArgumentException>(() => options.DistributedLockExpiration = value);
    }

    [Fact]
    public void VersionClock_is_strictly_increasing()
    {
        ulong previous = 0;
        for (var i = 0; i < 10_000; i++)
        {
            var next = ClickHouseVersionClock.Next();
            Assert.True(next > previous, $"expected strictly increasing versions, {next} <= {previous}");
            previous = next;
        }
    }

    [Fact]
    public void VersionClock_is_monotonic_under_concurrency()
    {
        var results = new System.Collections.Concurrent.ConcurrentBag<ulong>();

        Parallel.For(0, 16, _ =>
        {
            for (var i = 0; i < 5_000; i++)
                results.Add(ClickHouseVersionClock.Next());
        });

        var all = results.ToArray();
        Assert.Equal(all.Length, all.Distinct().Count()); // every stamp is unique
    }

    [Fact]
    public void Schema_qualifies_and_prefixes_table_names()
    {
        var schema = new ClickHouseSchema("hangfire", "hf_");
        Assert.Equal("`hangfire`.`hf_job`", schema.Job);
        Assert.Equal("`hangfire`.`hf_job_queue`", schema.JobQueue);
        Assert.Equal("`hangfire`.`hf_distributed_lock`", schema.DistributedLock);

        var noPrefix = new ClickHouseSchema("db", "");
        Assert.Equal("`db`.`set`", noPrefix.Set);
    }

    [Fact]
    public void Schema_emits_all_expected_create_statements()
    {
        var schema = new ClickHouseSchema("hangfire", string.Empty);
        var statements = ClickHouseObjectsInstaller.CreateStatements(schema).ToList();

        // one CREATE TABLE per storage table
        Assert.Equal(14, statements.Count);
        Assert.All(statements, s => Assert.Contains("CREATE TABLE IF NOT EXISTS", s));
        Assert.Contains(statements, s => s.Contains("ReplacingMergeTree(ver)"));
    }

    [Fact]
    public void State_data_round_trips()
    {
        var data = new Dictionary<string, string>
        {
            ["EnqueuedAt"] = "2026-06-12T10:00:00.000Z",
            ["Queue"] = "default",
        };

        var json = ClickHouseJobSerialization.SerializeStateData(data);
        var restored = ClickHouseJobSerialization.DeserializeStateData(json);

        Assert.Equal(data, restored);
    }
}
