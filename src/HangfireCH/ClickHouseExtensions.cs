using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Octonica.ClickHouseClient;
using Octonica.ClickHouseClient.Exceptions;

namespace HangfireCH;

/// <summary>
/// Thin helpers over Octonica that encode the validated conventions:
/// <list type="bullet">
/// <item><see cref="DateTime"/> parameters must use <see cref="DbType.DateTime2"/> or the
/// sub-second part of a <c>DateTime64</c> column is dropped.</item>
/// <item>Numeric reads go through <see cref="Convert"/> because ClickHouse aggregate columns
/// (e.g. <c>count()</c>) come back as <c>UInt64</c>, not <c>Int64</c>.</item>
/// <item>Mutations (<c>DELETE</c>/<c>ALTER</c>) must NOT use parameters — Octonica passes
/// parameters as temporary tables that no longer exist when the async mutation runs.</item>
/// </list>
/// </summary>
internal static class ClickHouseExtensions
{
    public static void AddParameter(this ClickHouseCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;

        switch (value)
        {
            case null:
            case DBNull:
                parameter.Value = DBNull.Value;
                break;
            case DateTime dateTime:
                parameter.DbType = DbType.DateTime2;
                parameter.Value = DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified);
                break;
            default:
                parameter.Value = value;
                break;
        }

        command.Parameters.Add(parameter);
    }

    public static ClickHouseCommand CreateCommand(this ClickHouseConnection connection, string sql,
        params (string Name, object? Value)[] parameters)
    {
        var command = connection.CreateCommand(sql);
        foreach (var (name, value) in parameters)
            command.AddParameter(name, value);
        return command;
    }

    public static int ExecuteNonQuery(this ClickHouseConnection connection, string sql,
        params (string Name, object? Value)[] parameters)
    {
        using var command = connection.CreateCommand(sql, parameters);
        return command.ExecuteNonQuery();
    }

    /// <summary>
    /// Inserts <paramref name="rows"/> into <paramref name="table"/>. When <paramref name="batch"/>
    /// is true they go in as a single multi-row parameterized INSERT (fewer, larger MergeTree
    /// parts); otherwise one INSERT per row. Each row's values align with <paramref name="columns"/>
    /// / <paramref name="types"/>. (Multi-row keeps Octonica's parameter temp-tables, unlike async_insert.)
    /// </summary>
    public static void InsertRows(this ClickHouseConnection connection, string table,
        string[] columns, string[] types, IReadOnlyList<object?[]> rows, bool batch)
    {
        if (rows.Count == 0) return;

        var columnList = string.Join(", ", columns);

        if (!batch)
        {
            foreach (var row in rows)
            {
                var ps = new (string, object?)[columns.Length];
                var placeholders = new string[columns.Length];
                for (var c = 0; c < columns.Length; c++)
                {
                    placeholders[c] = $"{{v{c}:{types[c]}}}";
                    ps[c] = ($"v{c}", row[c]);
                }
                connection.ExecuteNonQuery($"INSERT INTO {table} ({columnList}) VALUES ({string.Join(", ", placeholders)})", ps);
            }
            return;
        }

        var sql = new StringBuilder();
        sql.Append("INSERT INTO ").Append(table).Append(" (").Append(columnList).Append(") VALUES ");
        var parameters = new List<(string, object?)>(rows.Count * columns.Length);
        for (var r = 0; r < rows.Count; r++)
        {
            if (r > 0) sql.Append(", ");
            sql.Append('(');
            for (var c = 0; c < columns.Length; c++)
            {
                if (c > 0) sql.Append(", ");
                var name = $"r{r}c{c}";
                sql.Append('{').Append(name).Append(':').Append(types[c]).Append('}');
                parameters.Add((name, rows[r][c]));
            }
            sql.Append(')');
        }

        connection.ExecuteNonQuery(sql.ToString(), parameters.ToArray());
    }

    public static object? ExecuteScalar(this ClickHouseConnection connection, string sql,
        params (string Name, object? Value)[] parameters)
    {
        using var command = connection.CreateCommand(sql, parameters);
        var result = command.ExecuteScalar();
        return result is DBNull ? null : result;
    }

    public static long ExecuteCount(this ClickHouseConnection connection, string sql,
        params (string Name, object? Value)[] parameters)
    {
        var result = connection.ExecuteScalar(sql, parameters);
        return result is null ? 0L : Convert.ToInt64(result);
    }

    // ----- reader helpers (column types are fixed by the schema) -----

    public static string? GetNullableString(this ClickHouseDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    public static string GetStringOrEmpty(this ClickHouseDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? string.Empty : reader.GetString(ordinal);

    // Named Read* (not Get*) so they are NOT shadowed by the native DbDataReader.GetInt64 /
    // GetDouble instance methods, whose typed casts throw on ClickHouse UInt64 columns
    // (e.g. count()). Convert handles the unsigned-to-signed widening.
    public static long ReadInt64(this ClickHouseDataReader reader, int ordinal)
        => Convert.ToInt64(reader.GetValue(ordinal));

    public static double ReadDouble(this ClickHouseDataReader reader, int ordinal)
        => Convert.ToDouble(reader.GetValue(ordinal));

    public static DateTime GetUtcDateTime(this ClickHouseDataReader reader, int ordinal)
        => DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc);

    public static DateTime? GetNullableUtcDateTime(this ClickHouseDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc);

    /// <summary>
    /// Normalizes a scalar value from a <c>DateTime64('UTC')</c> column to a UTC
    /// <see cref="DateTime"/>. Octonica returns such columns as <see cref="DateTimeOffset"/>
    /// from <c>ExecuteScalar</c>/<c>GetValue</c> (the typed <c>GetDateTime</c> path returns
    /// <see cref="DateTime"/>).
    /// </summary>
    public static DateTime? ToUtcDateTime(object? value) => value switch
    {
        null => null,
        DateTimeOffset offset => offset.UtcDateTime,
        DateTime dateTime => DateTime.SpecifyKind(dateTime, DateTimeKind.Utc),
        _ => DateTime.SpecifyKind(Convert.ToDateTime(value), DateTimeKind.Utc),
    };

    /// <summary>True when the exception indicates the target table/database is missing.</summary>
    public static bool IsMissingObject(this ClickHouseServerException exception)
        => exception.Message.Contains("UNKNOWN_TABLE", StringComparison.OrdinalIgnoreCase)
        || exception.Message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
        || exception.Message.Contains("does not exist", StringComparison.OrdinalIgnoreCase);
}
