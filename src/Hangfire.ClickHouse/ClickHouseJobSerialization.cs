using System.Collections.Generic;
using Hangfire.Common;
using Hangfire.Storage;

namespace Hangfire.ClickHouse;

/// <summary>
/// Wraps Hangfire's (de)serialization helpers so the storage stores the same payload
/// shape as the built-in providers: an invocation-data payload (without arguments) plus a
/// separate arguments string, and state data as a JSON dictionary.
/// </summary>
internal static class ClickHouseJobSerialization
{
    public static (string InvocationData, string Arguments) SerializeJob(Job job)
    {
        var invocationData = InvocationData.SerializeJob(job);
        var payload = invocationData.SerializePayload(excludeArguments: true);
        return (payload, invocationData.Arguments);
    }

    public static (Job? Job, JobLoadException? LoadException) DeserializeJob(string? invocationData, string? arguments)
    {
        if (string.IsNullOrEmpty(invocationData))
            return (null, null);

        var data = InvocationData.DeserializePayload(invocationData);
        if (!string.IsNullOrEmpty(arguments))
            data.Arguments = arguments;

        try
        {
            return (data.DeserializeJob(), null);
        }
        catch (JobLoadException ex)
        {
            return (null, ex);
        }
    }

    public static string SerializeStateData(IDictionary<string, string>? data)
        => SerializationHelper.Serialize(data ?? new Dictionary<string, string>());

    public static Dictionary<string, string> DeserializeStateData(string? json)
    {
        if (string.IsNullOrEmpty(json))
            return new Dictionary<string, string>();

        return SerializationHelper.Deserialize<Dictionary<string, string>>(json)
               ?? new Dictionary<string, string>();
    }
}
