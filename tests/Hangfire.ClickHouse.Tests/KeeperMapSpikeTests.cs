using System;
using System.Threading.Tasks;
using Hangfire.ClickHouse.Tests.Infrastructure;
using Octonica.ClickHouseClient;
using Octonica.ClickHouseClient.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.ClickHouse.Tests;

/// <summary>
/// Validates the exact KeeperMap behaviours the opt-in lock/queue relies on: strict-mode
/// INSERT is an atomic create (duplicate key throws), and rows can be read back and deleted.
/// </summary>
[Collection("clickhouse-keeper")]
public sealed class KeeperMapSpikeTests
{
    private readonly KeeperClickHouseContainer _ch;
    private readonly ITestOutputHelper _out;

    public KeeperMapSpikeTests(KeeperClickHouseContainer ch, ITestOutputHelper output)
    {
        _ch = ch;
        _out = output;
    }

    [Fact]
    public async Task StrictInsert_is_atomic_create()
    {
        const string db = "kmspike";
        await using (var bootstrap = new ClickHouseConnection(_ch.ConnectionString("default")))
        {
            await bootstrap.OpenAsync();
            await Exec(bootstrap, $"CREATE DATABASE IF NOT EXISTS {db}");
        }

        // The server prepends its keeper_map_path_prefix, so the table path stays relative.
        await using (var setup = new ClickHouseConnection(_ch.ConnectionString(db)))
        {
            await setup.OpenAsync();
            await Exec(setup, "DROP TABLE IF EXISTS lock_kv");
            await Exec(setup, $"CREATE TABLE lock_kv (resource String, owner String) ENGINE = KeeperMap('/{db}/lock') PRIMARY KEY resource");
        }

        // First acquire (own connection, strict mode).
        await using (var conn = await Open(db))
            await Insert(conn, "res", "owner-A");

        // Duplicate acquire throws under strict mode — this connection is then broken.
        await using (var conn = await Open(db))
        {
            var threw = false;
            try { await Insert(conn, "res", "owner-B"); }
            catch (ClickHouseServerException ex) { threw = true; _out.WriteLine("duplicate insert threw: " + ex.Message.Split('\n')[0]); }
            Assert.True(threw, "strict-mode duplicate INSERT should throw");
        }

        // Fresh connection: owner is still A; DELETE (with parameter) releases it.
        await using (var conn = await Open(db))
        {
            Assert.Equal("owner-A", await Scalar(conn, "SELECT owner FROM lock_kv WHERE resource = {r:String}", ("r", "res")));
            await Exec(conn, "DELETE FROM lock_kv WHERE resource = {r:String}", ("r", "res"));
            Assert.Equal(0L, await Count(conn, "SELECT count() FROM lock_kv"));
        }

        // Re-acquire after release.
        await using (var conn = await Open(db))
        {
            await Insert(conn, "res", "owner-C");
            Assert.Equal("owner-C", await Scalar(conn, "SELECT owner FROM lock_kv WHERE resource = {r:String}", ("r", "res")));
        }
    }

    private async Task<ClickHouseConnection> Open(string db)
    {
        var conn = new ClickHouseConnection(_ch.ConnectionString(db));
        await conn.OpenAsync();
        await Exec(conn, "SET keeper_map_strict_mode = 1");
        return conn;
    }

    private static async Task Insert(ClickHouseConnection conn, string resource, string owner)
    {
        await using var cmd = conn.CreateCommand("INSERT INTO lock_kv (resource, owner) VALUES ({r:String}, {o:String})");
        cmd.Parameters.AddWithValue("r", resource);
        cmd.Parameters.AddWithValue("o", owner);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task Exec(ClickHouseConnection conn, string sql, params (string, object)[] ps)
    {
        await using var cmd = conn.CreateCommand(sql);
        foreach (var (n, v) in ps) cmd.Parameters.AddWithValue(n, v);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> Scalar(ClickHouseConnection conn, string sql, params (string, object)[] ps)
    {
        await using var cmd = conn.CreateCommand(sql);
        foreach (var (n, v) in ps) cmd.Parameters.AddWithValue(n, v);
        return (await cmd.ExecuteScalarAsync()) as string;
    }

    private static async Task<long> Count(ClickHouseConnection conn, string sql)
    {
        await using var cmd = conn.CreateCommand(sql);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }
}
