using System;
using Hangfire.ClickHouse;

namespace Hangfire;

/// <summary>
/// <c>UseClickHouseStorage</c> entry points, surfaced in the <c>Hangfire</c> namespace so
/// they appear alongside the other <c>UseXxxStorage</c> configuration methods.
/// </summary>
public static class ClickHouseStorageExtensions
{
    public static IGlobalConfiguration<ClickHouseStorage> UseClickHouseStorage(
        this IGlobalConfiguration configuration,
        string connectionString)
    {
        if (configuration is null) throw new ArgumentNullException(nameof(configuration));

        var storage = new ClickHouseStorage(connectionString);
        return configuration.UseStorage(storage);
    }

    public static IGlobalConfiguration<ClickHouseStorage> UseClickHouseStorage(
        this IGlobalConfiguration configuration,
        string connectionString,
        ClickHouseStorageOptions options)
    {
        if (configuration is null) throw new ArgumentNullException(nameof(configuration));

        var storage = new ClickHouseStorage(connectionString, options);
        return configuration.UseStorage(storage);
    }
}
