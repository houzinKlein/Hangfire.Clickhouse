using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading;
using Octonica.ClickHouseClient;

namespace Hangfire.ClickHouse;

/// <summary>
/// Creates and pools <see cref="ClickHouseConnection"/> instances. Octonica opens a
/// fresh TCP socket per connection and has no built-in ADO.NET pool, so we keep a small
/// bag of open connections and hand them out per logical operation.
/// </summary>
internal sealed class ClickHouseConnectionFactory : IDisposable
{
    private readonly string _connectionString;
    private readonly int _maxPoolSize;
    private readonly ConcurrentBag<ClickHouseConnection> _pool = new();
    private int _count;
    private int _disposed;

    public ClickHouseConnectionFactory(string connectionString, int maxPoolSize = 32)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _maxPoolSize = maxPoolSize;
    }

    /// <summary>Rents a connection. Dispose the returned lease to return it to the pool.</summary>
    public Lease Rent()
    {
        ThrowIfDisposed();

        while (_pool.TryTake(out var pooled))
        {
            Interlocked.Decrement(ref _count);
            if (pooled.State == ConnectionState.Open)
                return new Lease(this, pooled);

            SafeDispose(pooled);
        }

        var connection = new ClickHouseConnection(_connectionString);
        connection.Open();
        return new Lease(this, connection);
    }

    /// <summary>
    /// Opens a standalone connection that is NOT returned to the pool. Used for long-lived
    /// owners (e.g. the schema installer) where pooling brings no benefit.
    /// </summary>
    public ClickHouseConnection CreateAndOpen()
    {
        ThrowIfDisposed();
        var connection = new ClickHouseConnection(_connectionString);
        connection.Open();
        return connection;
    }

    private void Release(ClickHouseConnection connection)
    {
        if (Volatile.Read(ref _disposed) != 0 ||
            connection.State != ConnectionState.Open ||
            Interlocked.Increment(ref _count) > _maxPoolSize)
        {
            Interlocked.Decrement(ref _count);
            SafeDispose(connection);
            return;
        }

        _pool.Add(connection);
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(nameof(ClickHouseConnectionFactory));
    }

    private static void SafeDispose(ClickHouseConnection connection)
    {
        try { connection.Dispose(); }
        catch { /* best effort */ }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        while (_pool.TryTake(out var c)) SafeDispose(c);
    }

    /// <summary>A rented connection that returns itself to the pool when disposed.</summary>
    public readonly struct Lease : IDisposable
    {
        private readonly ClickHouseConnectionFactory _factory;

        public Lease(ClickHouseConnectionFactory factory, ClickHouseConnection connection)
        {
            _factory = factory;
            Connection = connection;
        }

        public ClickHouseConnection Connection { get; }

        public void Dispose() => _factory.Release(Connection);
    }
}
