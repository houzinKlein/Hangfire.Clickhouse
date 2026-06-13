using System;
using System.Threading;
using Hangfire.Storage;

namespace Hangfire.ClickHouse;

/// <summary>
/// A job claimed from the queue. A timer refreshes <c>fetched_at</c> so the entry stays
/// invisible to other workers while the job runs; on success Hangfire calls
/// <see cref="RemoveFromQueue"/>, on failure <see cref="Requeue"/>.
/// </summary>
internal sealed class ClickHouseFetchedJob : IFetchedJob
{
    private readonly ClickHouseStorage _storage;
    private readonly string _queue;
    private readonly string _token;
    private readonly DateTime _enqueuedAt;
    private readonly Timer _keepAlive;
    private readonly object _gate = new();
    private bool _completed;
    private bool _disposed;

    public ClickHouseFetchedJob(ClickHouseStorage storage, string queue, string jobId, string token, DateTime enqueuedAt)
    {
        _storage = storage;
        _queue = queue;
        JobId = jobId;
        _token = token;
        _enqueuedAt = enqueuedAt;

        var interval = TimeSpan.FromMilliseconds(Math.Max(1000, storage.Options.InvisibilityTimeout.TotalMilliseconds / 2));
        _keepAlive = new Timer(_ => OnKeepAlive(), null, interval, interval);
    }

    public string JobId { get; }

    public void RemoveFromQueue()
    {
        lock (_gate)
        {
            if (_completed) return;
            _completed = true;
            StopKeepAlive();
        }

        ClickHouseJobQueue.Remove(_storage, _queue, JobId, _token, _enqueuedAt);
    }

    public void Requeue()
    {
        lock (_gate)
        {
            if (_completed) return;
            _completed = true;
            StopKeepAlive();
        }

        ClickHouseJobQueue.Requeue(_storage, _queue, JobId, _enqueuedAt);
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            StopKeepAlive();
        }
    }

    private void OnKeepAlive()
    {
        lock (_gate)
        {
            if (_completed || _disposed) return;
        }

        try
        {
            ClickHouseJobQueue.Refresh(_storage, _queue, JobId, _token, _enqueuedAt);
        }
        catch
        {
            // best effort; the invisibility timeout is the backstop.
        }
    }

    private void StopKeepAlive()
    {
        try { _keepAlive.Dispose(); }
        catch { /* ignore */ }
    }
}
