# Time-series mechanisms over public Data values

`aggregate_time_series_samples` aggregates already-materialized samples into
fixed-width buckets. It preserves unknown quality and marks output provenance
as derived. Callers provide validated range/aggregation values and ordered input;
this helper does not fetch samples, infer missing observations or choose a clock.

`paginate` (also `timeseries::paginate`) preserves the existing public time-series
page/continuation behavior. `InMemoryDataTimeSeriesStore` provides reference
read/write semantics, snapshots, revision/idempotency handling and organization
isolation using the public Data TimeSeries Ports. Its state is process-local;
it is not a durable storage backend or selected-World acceptance implementation.

The owner retains provider routing, State/Signal semantic acceptance, World
selection and durable storage. The former `routed-timeseries` owner composition
is not part of this library. There is no dependency on mixed data-validation or
another implementation repository.

The initial implementations and applicable regressions come from
`hvac-workbench` commit `04b106a8467975276bfe70577b3d1e824521492e`:
`apps/adapters/univers-data-timeseries-builtin/src/timeseries.rs` and
`apps/common/univers-data-validation/src/timeseries_aggregation*.rs`.
The adapter now uses the aggregation function within this package; behavior is
otherwise retained.

Add `univers-aip-lib-timeseries = {version="=0.1.1",registry="univers"}`.
The release lock validates C0 Data rc.2. Run `bash scripts/check.sh`,
`bash scripts/build.sh`, and `bash scripts/publish.sh` from a clean committed
candidate. Registry credentials remain external.

Optional `state-history-adapter` exports
`DataTimeSeriesStateHistoryAdapter::new(Arc<dyn DataTimeSeriesQueryPort>)`.
It implements the public World `StateHistoryReadPort`, requires the Data
`StateQuery` capability and validates both requests and responses. It translates
state reads using LocalOnly and optional Last aggregation, preserving the exact
scope, snapshot, quality, provenance, lineage, warnings and completeness.
It owns no transport, backend selection, semantic evaluation or authorization.
Only the C0 Data and World contracts are needed.
