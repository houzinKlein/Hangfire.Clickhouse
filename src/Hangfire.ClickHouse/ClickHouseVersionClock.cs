using System;
using System.Threading;

namespace Hangfire.ClickHouse;

/// <summary>
/// Produces strictly increasing <c>UInt64</c> version stamps for the <c>ver</c> column of
/// the <c>ReplacingMergeTree</c> tables. Reads resolve the current value of a row with
/// <c>argMax(col, ver)</c>, so a higher <c>ver</c> must always mean "written later".
///
/// The stamp is seeded from the wall clock (100-ns ticks) so values are roughly comparable
/// across processes, and an atomic compare-exchange guarantees strict monotonicity within a
/// process even when the OS clock resolution is coarse (e.g. ~15 ms on Windows) or two calls
/// land in the same tick. Cross-process ordering for the same key in the same tick is
/// last-writer-wins and inherently best-effort — see the README "Design &amp; guarantees".
/// </summary>
internal static class ClickHouseVersionClock
{
    private static long _last;

    public static ulong Next()
    {
        while (true)
        {
            var previous = Interlocked.Read(ref _last);
            var candidate = Math.Max(DateTime.UtcNow.Ticks, previous + 1);
            if (Interlocked.CompareExchange(ref _last, candidate, previous) == previous)
                return (ulong)candidate;
        }
    }
}
