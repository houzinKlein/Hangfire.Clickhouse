using Xunit;

// These are container-bound integration tests. Running the (separate) "clickhouse" and
// "clickhouse-keeper" collections in parallel starts two ClickHouse containers at once and
// starves the CPU, which makes the contended KeeperMap lock/queue operations time out. Run
// collections sequentially so only one container is active at a time.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
