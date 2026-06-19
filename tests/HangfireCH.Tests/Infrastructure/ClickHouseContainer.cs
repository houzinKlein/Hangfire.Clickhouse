using System;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Octonica.ClickHouseClient;
using Xunit;

namespace HangfireCH.Tests.Infrastructure;

/// <summary>
/// Spins up a single ClickHouse server container shared across a test collection.
/// Octonica talks the native protocol (port 9000); we wait both for the "Ready for
/// connections" log line and for a real native-protocol query to succeed, because the
/// HTTP /ping endpoint comes up before the native listener finishes user provisioning.
/// </summary>
public sealed class ClickHouseContainer : IAsyncLifetime
{
    private const string User = "hangfire";
    private const string Password = "hangfire";

    // Overridable via the CLICKHOUSE_IMAGE env var so CI can matrix across server versions.
    // Octonica 4.1.4 requires 24.12+ (older versions dead-lock during the native handshake).
    public static string Image =>
        Environment.GetEnvironmentVariable("CLICKHOUSE_IMAGE") is { Length: > 0 } image
            ? image
            : "clickhouse/clickhouse-server:24.12";

    private IContainer _container = null!;

    public string Host { get; private set; } = "localhost";

    public ushort NativePort { get; private set; }

    public async Task InitializeAsync()
    {
        // Octonica 4.1.4's native protocol handshake requires ClickHouse 24.12+; it
        // dead-locks during the hello/addendum exchange against 24.8 and older.
#pragma warning disable CS0618 // generic ContainerBuilder() is fine for an ad-hoc image
        _container = new ContainerBuilder()
            .WithImage(Image)
            .WithPortBinding(9000, true)
            .WithPortBinding(8123, true)
            // Provision an explicit user so we don't depend on the image's default-user policy.
            .WithEnvironment("CLICKHOUSE_USER", User)
            .WithEnvironment("CLICKHOUSE_PASSWORD", Password)
            .WithEnvironment("CLICKHOUSE_DB", "default")
            // /ping (HTTP) signals the container is up; the native protocol (9000) finishes
            // a moment later, which WaitForNativeProtocolAsync below covers. Do NOT use
            // UntilMessageIsLogged: the official image logs to a file, not stdout, so the
            // "Ready for connections" line never reaches the container's stdout stream.
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r.ForPort(8123).ForPath("/ping")))
            .Build();
#pragma warning restore CS0618

        await _container.StartAsync();

        Host = _container.Hostname;
        NativePort = _container.GetMappedPublicPort(9000);

        await WaitForNativeProtocolAsync();
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    public string ConnectionString(string database) =>
        $"Host={Host};Port={NativePort};User={User};Password={Password};Database={database}";

    private async Task WaitForNativeProtocolAsync()
    {
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

        throw new InvalidOperationException("ClickHouse native protocol did not become ready in time.", last);
    }
}
