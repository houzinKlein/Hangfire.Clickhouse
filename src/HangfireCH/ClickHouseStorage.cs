using System;
using System.Collections.Generic;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Octonica.ClickHouseClient;

namespace HangfireCH;

/// <summary>
/// Hangfire <see cref="JobStorage"/> backed by ClickHouse via the Octonica client.
/// </summary>
public class ClickHouseStorage : JobStorage, IDisposable
{
    private static readonly ILog Logger = LogProvider.GetLogger(typeof(ClickHouseStorage));

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
        _factory = new ClickHouseConnectionFactory(runtime, options.ConnectionPoolSize);
        Schema = new ClickHouseSchema(database, options.TablePrefix, options.KeeperMapPathPrefix);

        if (options.PrepareSchemaIfNecessary)
        {
            using var connection = new ClickHouseConnection(_installConnectionString);
            connection.Open();
            ClickHouseObjectsInstaller.InstallAsync(connection, Schema, options.UseKeeperMap).GetAwaiter().GetResult();
            Logger.InfoFormat("ClickHouse schema v{0} ready in database '{1}' (keeperMap={2}).",
                ClickHouseObjectsInstaller.SchemaVersion, database, options.UseKeeperMap);
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

    public override void WriteOptionsToLog(ILog logger)
    {
        logger.Info("Using the following options for ClickHouse job storage:");
        logger.InfoFormat("    Database: {0}, TablePrefix: '{1}'", Schema.Database, Options.TablePrefix);
        logger.InfoFormat("    QueuePollInterval: {0}, InvisibilityTimeout: {1}", Options.QueuePollInterval, Options.InvisibilityTimeout);
        logger.InfoFormat("    JobExpirationCheckInterval: {0}, CountersAggregateInterval: {1}",
            Options.JobExpirationCheckInterval, Options.CountersAggregateInterval);
        logger.InfoFormat("    BatchWrites: {0}, UseKeeperMap: {1}, ConnectionPoolSize: {2}, MutationsSync: {3}",
            Options.BatchWrites, Options.UseKeeperMap, Options.ConnectionPoolSize, Options.MutationsSync);
    }

    public override string ToString() => $"ClickHouse: {Schema.Database}";

    public void Dispose()
    {
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _factory.Dispose();
        GC.SuppressFinalize(this);
    }

    private static (string Runtime, string Install, string Database) Resolve(string connectionString, string? databaseOverride)
    {
        ClickHouseConnectionStringBuilder source;
        try
        {
            source = new ClickHouseConnectionStringBuilder(connectionString);
        }
        catch (Exception ex)
        {
            throw new ArgumentException("The ClickHouse connection string could not be parsed. " +
                "Expected Octonica native-protocol format, e.g. 'Host=...;Port=9000;User=...;Password=...;Database=...'.", nameof(connectionString), ex);
        }

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
