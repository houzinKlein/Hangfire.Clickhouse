using System;
using System.Data;
using Octonica.ClickHouseClient;
using Octonica.ClickHouseClient.Exceptions;

namespace Hangfire.ClickHouse;

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
