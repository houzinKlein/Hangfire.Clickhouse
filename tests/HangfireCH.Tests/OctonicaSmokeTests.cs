using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using HangfireCH.Tests.Infrastructure;
using Octonica.ClickHouseClient;
using Xunit;

namespace HangfireCH.Tests;

/// <summary>
/// Validates the exact Octonica.ClickHouseClient API surface and ClickHouse type
/// behaviours the storage relies on, against a real server. If these pass, the
/// storage building blocks (parameterized insert, bulk column writer, argMax reads,
/// Nullable / UInt64 / DateTime64 mapping, lightweight DELETE) are sound.
/// </summary>
[Collection("clickhouse")]
public sealed class OctonicaSmokeTests
{
    private readonly ClickHouseContainer _ch;

    public OctonicaSmokeTests(ClickHouseContainer ch) => _ch = ch;

    private async Task<ClickHouseConnection> OpenAsync(string db)
    {
        // ensure database exists using the default db
        await using (var bootstrap = new ClickHouseConnection(_ch.ConnectionString("default")))
        {
            await bootstrap.OpenAsync();
            await using var create = bootstrap.CreateCommand($"CREATE DATABASE IF NOT EXISTS {db}");
            await create.ExecuteNonQueryAsync();
        }

        var conn = new ClickHouseConnection(_ch.ConnectionString(db));
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task ConnectionStringBuilder_exposes_expected_properties()
    {
        var sb = new ClickHouseConnectionStringBuilder
        {
            Host = "localhost",
            Port = 9000,
            User = "default",
            Database = "default"
        };
        Assert.Equal("localhost", sb.Host);
        Assert.Equal((ushort)9000, sb.Port);
        Assert.Equal("default", sb.User);
        Assert.Equal("default", sb.Database);
    }

    [Fact]
    public async Task Parameterized_insert_and_argMax_read_roundtrip()
    {
        await using var conn = await OpenAsync("smoke_param");

        await Exec(conn, "DROP TABLE IF EXISTS t");
        await Exec(conn, @"CREATE TABLE t (
                id String,
                state String,
                score Float64,
                created_at DateTime64(6,'UTC'),
                expire_at Nullable(DateTime64(6,'UTC')),
                ver UInt64
            ) ENGINE = ReplacingMergeTree(ver) ORDER BY id");

        var created = new DateTime(2026, 6, 12, 10, 30, 15, DateTimeKind.Utc).AddTicks(1234560);

        // first version
        await InsertRow(conn, "job-1", "Enqueued", 1.5, created, null, 1);
        // second version -> latest state should win via argMax(ver)
        await InsertRow(conn, "job-1", "Processing", 2.5, created, created.AddDays(1), 2);

        await using var cmd = conn.CreateCommand(
            "SELECT argMax(state, ver) AS s, argMax(expire_at, ver) AS e, argMax(score, ver) AS sc, " +
            "argMax(created_at, ver) AS c FROM t WHERE id = {id:String} GROUP BY id");
        cmd.Parameters.AddWithValue("id", "job-1");

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("Processing", reader.GetString(0));
        Assert.False(reader.IsDBNull(1));
        var roundExpire = reader.GetDateTime(1);
        Assert.Equal(created.AddDays(1), roundExpire, TimeSpan.FromMilliseconds(1));
        Assert.Equal(2.5, reader.GetDouble(2));
        var roundCreated = reader.GetDateTime(3);
        Assert.Equal(created, roundCreated, TimeSpan.FromMilliseconds(1));
    }

    [Fact]
    public async Task Null_parameter_roundtrips_as_dbnull()
    {
        await using var conn = await OpenAsync("smoke_null");
        await Exec(conn, "DROP TABLE IF EXISTS t");
        await Exec(conn, "CREATE TABLE t (id String, expire_at Nullable(DateTime64(6,'UTC')), ver UInt64) ENGINE = MergeTree ORDER BY id");

        await InsertNullable(conn, "a", null, 1);
        await InsertNullable(conn, "b", new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), 1);

        await using var cmd = conn.CreateCommand("SELECT id, expire_at FROM t ORDER BY id");
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("a", reader.GetString(0));
        Assert.True(reader.IsDBNull(1));

        Assert.True(await reader.ReadAsync());
        Assert.Equal("b", reader.GetString(0));
        Assert.False(reader.IsDBNull(1));
    }

    [Fact]
    public async Task Bulk_column_writer_inserts_rows()
    {
        await using var conn = await OpenAsync("smoke_bulk");
        await Exec(conn, "DROP TABLE IF EXISTS t");
        await Exec(conn, "CREATE TABLE t (queue String, job_id String, ver UInt64) ENGINE = MergeTree ORDER BY (queue, job_id)");

        var queues = new[] { "default", "default", "critical" };
        var jobIds = new[] { "1", "2", "3" };
        var vers = new ulong[] { 1, 1, 1 };

        await using (var writer = await conn.CreateColumnWriterAsync(
            "INSERT INTO t (queue, job_id, ver) VALUES", default))
        {
            await writer.WriteTableAsync(new object[] { queues, jobIds, vers }, queues.Length, default);
            await writer.EndWriteAsync(default);
        }

        Assert.Equal(3L, await Scalar<long>(conn, "SELECT count() FROM t"));
        Assert.Equal(2L, await Scalar<long>(conn, "SELECT count() FROM t WHERE queue = {q:String}", ("q", "default")));
    }

    [Fact]
    public async Task Mutation_delete_with_server_side_predicate_removes_rows()
    {
        // CRITICAL: Octonica passes parameters as temporary tables, which do NOT exist when
        // a DELETE/ALTER runs as an async mutation. So mutations must use server-side
        // predicates only (now64(), INTERVAL, subqueries) — never client parameters.
        await using var conn = await OpenAsync("smoke_delete");
        await Exec(conn, "DROP TABLE IF EXISTS t");
        await Exec(conn, "CREATE TABLE t (k String, ver UInt64, expire_at Nullable(DateTime64(6,'UTC'))) ENGINE = MergeTree ORDER BY k");

        await InsertDeleteRow(conn, "keep", 1, null);
        await InsertDeleteRow(conn, "exp1", 1, new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        await InsertDeleteRow(conn, "exp2", 1, new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        // parameterless predicate using server-side now64()
        await Exec(conn, "DELETE FROM t WHERE expire_at IS NOT NULL AND expire_at < now64(6)");
        Assert.Equal(1L, await Scalar<long>(conn, "SELECT count() FROM t"));

        // subquery predicate (the cascade-delete pattern) — also parameterless
        await Exec(conn, "DELETE FROM t WHERE k IN (SELECT k FROM t)");
        Assert.Equal(0L, await Scalar<long>(conn, "SELECT count() FROM t"));
    }

    private static async Task InsertDeleteRow(ClickHouseConnection conn, string k, ulong ver, DateTime? expire)
    {
        await using var cmd = conn.CreateCommand(
            "INSERT INTO t (k, ver, expire_at) VALUES ({k:String},{v:UInt64},{e:Nullable(DateTime64(6))})");
        cmd.Parameters.AddWithValue("k", k);
        cmd.Parameters.AddWithValue("v", ver);
        AddNullableDateTime(cmd, "e", expire);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task InsertRow(ClickHouseConnection conn, string id, string state, double score, DateTime created, DateTime? expire, ulong ver)
    {
        await using var cmd = conn.CreateCommand(
            "INSERT INTO t (id, state, score, created_at, expire_at, ver) VALUES " +
            "({id:String},{state:String},{score:Float64},{created:DateTime64(6)},{expire:Nullable(DateTime64(6))},{ver:UInt64})");
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("state", state);
        cmd.Parameters.AddWithValue("score", score);
        AddNullableDateTime(cmd, "created", created); // DateTime64 precision requires DbType.DateTime2
        AddNullableDateTime(cmd, "expire", expire);
        cmd.Parameters.AddWithValue("ver", ver);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task InsertNullable(ClickHouseConnection conn, string id, DateTime? expire, ulong ver)
    {
        await using var cmd = conn.CreateCommand(
            "INSERT INTO t (id, expire_at, ver) VALUES ({id:String},{expire:Nullable(DateTime64(6))},{ver:UInt64})");
        cmd.Parameters.AddWithValue("id", id);
        AddNullableDateTime(cmd, "expire", expire);
        cmd.Parameters.AddWithValue("ver", ver);
        await cmd.ExecuteNonQueryAsync();
    }

    private static void AddNullableDateTime(ClickHouseCommand cmd, string name, DateTime? value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.DbType = DbType.DateTime2;
        p.Value = (object?)value ?? DBNull.Value;
        cmd.Parameters.Add(p);
    }

    private static async Task Exec(ClickHouseConnection conn, string sql, params (string, object)[] ps)
    {
        await using var cmd = conn.CreateCommand(sql);
        foreach (var (n, v) in ps) cmd.Parameters.AddWithValue(n, v);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<T> Scalar<T>(ClickHouseConnection conn, string sql, params (string, object)[] ps)
    {
        await using var cmd = conn.CreateCommand(sql);
        foreach (var (n, v) in ps) cmd.Parameters.AddWithValue(n, v);
        var result = await cmd.ExecuteScalarAsync();
        return (T)Convert.ChangeType(result!, typeof(T));
    }
}

[CollectionDefinition("clickhouse")]
public sealed class ClickHouseCollection : ICollectionFixture<ClickHouseContainer>
{
}
