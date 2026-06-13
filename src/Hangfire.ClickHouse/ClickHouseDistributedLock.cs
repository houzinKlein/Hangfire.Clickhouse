using System;

namespace Hangfire.ClickHouse;

/// <summary>
/// Released-on-dispose handle for a best-effort distributed lock. Release writes a
/// <c>released=1</c> version; if the owner crashes, the lock's TTL frees it instead.
/// </summary>
internal sealed class ClickHouseDistributedLock : IDisposable
{
    private readonly ClickHouseStorage _storage;
    private readonly string _resource;
    private readonly string _owner;
    private int _disposed;

    public ClickHouseDistributedLock(ClickHouseStorage storage, string resource, string owner)
    {
        _storage = storage;
        _resource = resource;
        _owner = owner;
    }

    public void Dispose()
    {
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0) return;

        try
        {
            _storage.UseConnection(connection => connection.ExecuteNonQuery(
                $@"INSERT INTO {_storage.Schema.DistributedLock} (resource, owner, acquired_at, expire_at, released, ver)
                   VALUES ({{resource:String}}, {{owner:String}}, now64(6), now64(6), 1, {{ver:UInt64}})",
                ("resource", _resource), ("owner", _owner), ("ver", ClickHouseVersionClock.Next())));
        }
        catch
        {
            // best effort: a stuck lock expires via its TTL.
        }
    }
}
