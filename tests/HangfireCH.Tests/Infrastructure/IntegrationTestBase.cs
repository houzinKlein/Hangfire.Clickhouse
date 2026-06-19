using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using HangfireCH;
using Hangfire.States;
using Hangfire.Storage;
using Xunit;

namespace HangfireCH.Tests.Infrastructure;

/// <summary>Base for tests that need a real <see cref="ClickHouseStorage"/> on a fresh database.</summary>
[Collection("clickhouse")]
public abstract class IntegrationTestBase
{
    private static readonly ConcurrentDictionary<string, ManualResetEventSlim> Signals = new();

    protected readonly ClickHouseContainer Ch;

    protected IntegrationTestBase(ClickHouseContainer ch) => Ch = ch;

    protected ClickHouseStorage CreateStorage(Action<ClickHouseStorageOptions>? configure = null)
    {
        var options = new ClickHouseStorageOptions
        {
            DatabaseName = "t_" + Guid.NewGuid().ToString("N"),
            QueuePollInterval = TimeSpan.FromMilliseconds(200),
            InvisibilityTimeout = TimeSpan.FromSeconds(15),
        };
        configure?.Invoke(options);
        return new ClickHouseStorage(Ch.ConnectionString("default"), options);
    }

    /// <summary>
    /// Hangfire's set/hash/list/counter read methods live on <see cref="JobStorageConnection"/>
    /// (the base), not the <see cref="IStorageConnection"/> interface, so tests use the base type.
    /// </summary>
    protected static JobStorageConnection Connect(ClickHouseStorage storage)
        => (JobStorageConnection)storage.GetConnection();

    protected static void InTransaction(IStorageConnection connection, Action<JobStorageTransaction> body)
    {
        using var transaction = (JobStorageTransaction)connection.CreateWriteTransaction();
        body(transaction);
        transaction.Commit();
    }

    // ----- targets for end-to-end job execution -----

    public static ManualResetEventSlim Signal(string runId) => Signals.GetOrAdd(runId, _ => new ManualResetEventSlim(false));

    public static void MarkDone(string runId) => Signal(runId).Set();

    public static void Throw(string message) => throw new InvalidOperationException(message);

    // ----- sample job target -----

    public static void Sample(string value) => _ = value;

    protected sealed class TestState : IState
    {
        public TestState(string name, string? reason = null, Dictionary<string, string>? data = null)
        {
            Name = name;
            Reason = reason;
            _data = data ?? new Dictionary<string, string>();
        }

        private readonly Dictionary<string, string> _data;

        public string Name { get; }
        public string? Reason { get; }
        public bool IsFinal => false;
        public bool IgnoreJobLoadException => false;
        public Dictionary<string, string> SerializeData() => _data;
    }
}
