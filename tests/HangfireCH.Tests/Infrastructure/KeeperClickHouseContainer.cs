using System;
using System.Text;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Octonica.ClickHouseClient;
using Xunit;

namespace HangfireCH.Tests.Infrastructure;

/// <summary>
/// A ClickHouse container with embedded Keeper enabled and <c>keeper_map_path_prefix</c> set,
/// so the linearizable <c>KeeperMap</c> engine (used by the opt-in lock/queue path) is usable.
/// </summary>
public sealed class KeeperClickHouseContainer : IAsyncLifetime
{
    public const string KeeperMapPathPrefix = "/keeper_map";

    private const string User = "hangfire";
    private const string Password = "hangfire";

    private const string KeeperConfig = """
        <clickhouse>
            <keeper_server>
                <tcp_port>9181</tcp_port>
                <server_id>1</server_id>
                <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
                <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
                <coordination_settings>
                    <operation_timeout_ms>10000</operation_timeout_ms>
                    <session_timeout_ms>30000</session_timeout_ms>
                    <raft_logs_level>warning</raft_logs_level>
                </coordination_settings>
                <raft_configuration>
                    <server><id>1</id><hostname>127.0.0.1</hostname><port>9234</port></server>
                </raft_configuration>
            </keeper_server>
            <zookeeper>
                <node><host>127.0.0.1</host><port>9181</port></node>
            </zookeeper>
            <keeper_map_path_prefix>/keeper_map</keeper_map_path_prefix>
        </clickhouse>
        """;

    private IContainer _container = null!;

    public string Host { get; private set; } = "localhost";

    public ushort NativePort { get; private set; }

    public async Task InitializeAsync()
    {
#pragma warning disable CS0618
        _container = new ContainerBuilder()
            .WithImage(ClickHouseContainer.Image)
            .WithPortBinding(9000, true)
            .WithPortBinding(8123, true)
            .WithEnvironment("CLICKHOUSE_USER", User)
            .WithEnvironment("CLICKHOUSE_PASSWORD", Password)
            .WithEnvironment("CLICKHOUSE_DB", "default")
            .WithResourceMapping(Encoding.UTF8.GetBytes(KeeperConfig), "/etc/clickhouse-server/config.d/keeper.xml")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r.ForPort(8123).ForPath("/ping")))
            .Build();
#pragma warning restore CS0618

        await _container.StartAsync();
        Host = _container.Hostname;
        NativePort = _container.GetMappedPublicPort(9000);

        Exception? last = null;
        for (var attempt = 0; attempt < 40; attempt++)
        {
            try
            {
                await using var conn = new ClickHouseConnection(ConnectionString("default"));
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

        throw new InvalidOperationException("Keeper-enabled ClickHouse did not become ready in time.", last);
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    public string ConnectionString(string database) =>
        $"Host={Host};Port={NativePort};User={User};Password={Password};Database={database}";
}

[CollectionDefinition("clickhouse-keeper")]
public sealed class KeeperClickHouseCollection : ICollectionFixture<KeeperClickHouseContainer>
{
}
