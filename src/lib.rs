//! Time-series aggregation, stable pagination and an explicit in-memory Port adapter.
//! No World selection, provider routing, durable store or semantic write acceptance.
pub mod timeseries;
mod timeseries_aggregation;
pub use timeseries::{paginate, InMemoryDataTimeSeriesStore};
pub use timeseries_aggregation::aggregate_time_series_samples;
#[cfg(test)]
mod timeseries_aggregation_tests;

#[cfg(feature = "state-history-adapter")]
pub mod state_history;
#[cfg(feature = "state-history-adapter")]
pub use state_history::DataTimeSeriesStateHistoryAdapter;
