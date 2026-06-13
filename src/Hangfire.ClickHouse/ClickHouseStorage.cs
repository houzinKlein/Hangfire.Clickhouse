using System;
using System.Collections.Generic;
using Hangfire.Server;
using Hangfire.Storage;
using Octonica.ClickHouseClient;

namespace Hangfire.ClickHouse;

/// <summary>
/// Hangfire <see cref="JobStorage"/> backed by ClickHouse via the Octonica client.
/// </summary>
public class ClickHouseStorage : JobStorage, IDisposable
{
    private readonly ClickHouseConnectionFactory _factory;
    private readonly string _installConnectionString;
    private int _disposed;

    /// <summary>Creates the storage from an Octonica connection string (native protocol, port 9000).</summary>
    public ClickHouseStorage(string connectionString)
        : this(connectionString, new ClickHouseStorageOptions())
    {
    }

    public ClickHouseStorage(string connectionString, ClickHouseStorageOptions options)
    {
        if (connectionString is null) throw new ArgumentNullException(nameof(connectionString));
        Options = options ?? throw new ArgumentNullException(nameof(options));

        var (runtime, install, database) = Resolve(connectionString, options.DatabaseName);
        _installConnectionString = install;
        _factory = new ClickHouseConnectionFactory(runtime);
        Schema = new ClickHouseSchema(database, options.TablePrefix);

        if (options.PrepareSchemaIfNecessary)
        {
            using var connection = new ClickHouseConnection(_installConnectionString);
            connection.Open();
            ClickHouseObjectsInstaller.InstallAsync(connection, Schema).GetAwaiter().GetResult();
        }
    }

    internal ClickHouseStorageOptions Options { get; }

    internal ClickHouseSchema Schema { get; }

    /// <summary>Rents a pooled connection, runs <paramref name="func"/>, and returns the connection.</summary>
    internal T UseConnection<T>(Func<ClickHouseConnection, T> func)
    {
        using var lease = _factory.Rent();
        return func(lease.Connection);
    }

    internal void UseConnection(Action<ClickHouseConnection> action)
    {
        using var lease = _factory.Rent();
        action(lease.Connection);
    }

    public override IStorageConnection GetConnection() => new ClickHouseStorageConnection(this);

    public override IMonitoringApi GetMonitoringApi() => new ClickHouseMonitoringApi(this);

#pragma warning disable CS0618 // GetComponents is obsolete; GetServerRequiredProcesses is the 1.8 path.
    public override IEnumerable<IBackgroundProcess> GetServerRequiredProcesses()
    {
        yield return new ExpirationManager(this);
        yield return new CountersAggregator(this);
    }
#pragma warning restore CS0618

    public override string ToString() => $"ClickHouse: {Schema.Database}";

    public void Dispose()
    {
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _factory.Dispose();
        GC.SuppressFinalize(this);
    }

    private static (string Runtime, string Install, string Database) Resolve(string connectionString, string? databaseOverride)
    {
        var source = new ClickHouseConnectionStringBuilder(connectionString);

        var database = !string.IsNullOrWhiteSpace(databaseOverride)
            ? databaseOverride!
            : !string.IsNullOrWhiteSpace(source.Database)
                ? source.Database!
                : "hangfire";

        var runtime = new ClickHouseConnectionStringBuilder(connectionString) { Database = database };

        // The schema installer creates the target database, so it must connect to one that
        // already exists. "default" is always present.
        var install = new ClickHouseConnectionStringBuilder(connectionString) { Database = "default" };

        return (runtime.ConnectionString, install.ConnectionString, database);
    }
}
