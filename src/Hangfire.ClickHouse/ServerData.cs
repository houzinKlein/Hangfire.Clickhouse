using System;

namespace Hangfire.ClickHouse;

/// <summary>Serialized payload stored in the <c>server.data</c> column.</summary>
internal sealed class ServerData
{
    public int WorkerCount { get; set; }

    public string[]? Queues { get; set; }

    public DateTime? StartedAt { get; set; }
}
