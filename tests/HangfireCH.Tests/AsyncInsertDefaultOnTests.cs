using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using HangfireCH.Tests.Infrastructure;
using Hangfire.Common;
using Hangfire.Storage;
using Octonica.ClickHouseClient;
using Xunit;

namespace HangfireCH.Tests;

/// <summary>
/// Regression for the production failure
/// "Unknown table expression identifier '_…' … While executing WaitForAsyncInsert".
///
/// When the ClickHouse server enables <c>async_insert</c> by default (a profile/user setting),
/// Octonica's parameterized inserts break: the parameters travel as a temporary table that no
/// longer exists when the deferred async insert is flushed. <see cref="ClickHouseStorage"/> must
/// force <c>async_insert = 0</c> on every connection so its inserts (distributed lock, jobs, …)
/// keep working regardless of the server default.
/// </summary>
public sealed class AsyncInsertDefaultOnTests : IAsyncLifetime
{
    private const string User = "hangfire";
    private const string Password = "hangfire";

    // Turn async_insert on by default for the (default) profile — i.e. simulate the prod server.
    private static readonly byte[] AsyncInsertProfile = Encoding.UTF8.GetBytes(
        "<clickhouse><profiles><default>" +
        "<async_insert>1</async_insert><wait_for_async_insert>1</wait_for_async_insert>" +
        "</default></profiles></clickhouse>");

    private IContainer _container = null!;
    private string _connectionString = null!;

    public async Task InitializeAsync()
    {
#pragma warning disable CS0618 // generic ContainerBuilder() is fine for an ad-hoc image
        _container = new ContainerBuilder()
            .WithImage(ClickHouseContainer.Image)
            .WithPortBinding(9000, true)
            .WithPortBinding(8123, true)
            .WithEnvironment("CLICKHOUSE_USER", User)
            .WithEnvironment("CLICKHOUSE_PASSWORD", Password)
            .WithEnvironment("CLICKHOUSE_DB", "default")
            .WithResourceMapping(AsyncInsertProfile, "/etc/clickhouse-server/users.d/zz-async-insert.xml")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r.ForPort(8123).ForPath("/ping")))
            .Build();
#pragma warning restore CS0618

        await _container.StartAsync();
        var port = _container.GetMappedPublicPort(9000);
        _connectionString = $"Host={_container.Hostname};Port={port};User={User};Password={Password};Database=default";

        await WaitForNativeProtocolAsync();
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    [Fact]
    public void Storage_operations_work_when_server_defaults_async_insert_on()
    {
        // Sanity: confirm the server really does default async_insert on, so this test exercises
        // the real condition rather than silently passing.
        using (var raw = new ClickHouseConnection(_connectionString))
        {
            raw.Open();
            using var cmd = raw.CreateCommand("SELECT toUInt8(getSetting('async_insert'))");
            Assert.Equal(1, Convert.ToInt32(cmd.ExecuteScalar()));
        }

        var options = new ClickHouseStorageOptions
        {
            DatabaseName = "t_" + Guid.NewGuid().ToString("N"),
            InvisibilityTimeout = TimeSpan.FromSeconds(15),
        };

        using var storage = new ClickHouseStorage(_connectionString, options);
        using var connection = storage.GetConnection();

        // (1) distributed lock — the exact prod path (parameterized INSERT … SELECT resource FROM <temp>).
        using (connection.AcquireDistributedLock("regression-resource", TimeSpan.FromSeconds(10)))
        {
            // (2) job creation — the batched InsertRows parameterized path.
            var jobId = connection.CreateExpiredJob(
                Job.FromExpression(() => IntegrationTestBase.Sample("x")),
                new Dictionary<string, string> { ["p"] = "v" },
                DateTime.UtcNow,
                TimeSpan.FromMinutes(5));

            Assert.False(string.IsNullOrEmpty(jobId));
            Assert.NotNull(connection.GetJobData(jobId));
        }
    }

    private async Task WaitForNativeProtocolAsync()
    {
        Exception? last = null;
        for (var attempt = 0; attempt < 40; attempt++)
        {
            try
            {
                await using var conn = new ClickHouseConnection(_connectionString);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand("SELECT 1");
                await cmd.ExecuteScalarAsync();
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(500);
            }
        }

        throw new InvalidOperationException("ClickHouse native protocol did not become ready in time.", last);
    }
}
