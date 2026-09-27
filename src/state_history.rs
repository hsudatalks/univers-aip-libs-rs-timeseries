//! Pure adapter from the Data query Port to the World state-history Port.
use std::sync::Arc;
#[cfg(test)]
use univers_aip_contracts_data::core::{DataKind, DataQuerySnapshot, DataRef, TimeRange};
use univers_aip_contracts_data::core::{DataPointError, DataPointResult};
use univers_aip_contracts_world::state::{
    StateHistoryQuery, StateHistoryReadPort, StateObservation,
};
mod timeseries_adapter {
    use async_trait::async_trait;
    use univers_aip_contracts_data::timeseries::{
        validate_state_time_series_batch, validate_time_series_batch_result,
        DataTimeSeriesAggregation, DataTimeSeriesAggregationFunction, DataTimeSeriesCapability,
        DataTimeSeriesPage, DataTimeSeriesQuery, DataTimeSeriesQueryPort,
        DataTimeSeriesSourcePolicy, DataTimeSeriesValue,
    };
    use univers_aip_contracts_world::state::{
        validate_state_history_batch, validate_state_history_batch_result, StateHistoryPage,
        StateValue,
    };

    use super::{
        Arc, DataPointError, DataPointResult, StateHistoryQuery, StateHistoryReadPort,
        StateObservation,
    };

    pub struct DataTimeSeriesStateHistoryAdapter {
        inner: Arc<dyn DataTimeSeriesQueryPort>,
    }

    impl DataTimeSeriesStateHistoryAdapter {
        pub fn new(inner: Arc<dyn DataTimeSeriesQueryPort>) -> DataPointResult<Self> {
            if !inner
                .query_capabilities()
                .contains(&DataTimeSeriesCapability::StateQuery)
            {
                return Err(DataPointError::NotSupported(
                    "State history requires the canonical StateQuery capability".to_string(),
                ));
            }
            Ok(Self { inner })
        }
    }

    #[async_trait]
    impl StateHistoryReadPort for DataTimeSeriesStateHistoryAdapter {
        async fn read_batch(
            &self,
            queries: Vec<StateHistoryQuery>,
        ) -> DataPointResult<Vec<StateHistoryPage>> {
            validate_state_history_batch(&queries)?;
            let physical = queries
                .iter()
                .map(|query| {
                    let mut physical = DataTimeSeriesQuery::new(
                        query.reference.clone(),
                        query.range.clone(),
                        query.snapshot,
                    )
                    .with_source_policy(DataTimeSeriesSourcePolicy::LocalOnly)
                    .with_limit(query.limit)?;
                    if let Some(resolution) = query.resolution {
                        physical = physical.with_aggregation(DataTimeSeriesAggregation::new(
                            resolution,
                            DataTimeSeriesAggregationFunction::Last,
                        )?)?;
                    }
                    Ok(physical)
                })
                .collect::<DataPointResult<Vec<_>>>()?;
            validate_state_time_series_batch(&physical)?;
            let pages = self.inner.query_batch(physical.clone()).await?;
            validate_time_series_batch_result(&physical, &pages)?;
            let pages = pages.into_iter().map(map_page).collect::<Vec<_>>();
            validate_state_history_batch_result(&queries, &pages)?;
            Ok(pages)
        }
    }

    fn map_page(page: DataTimeSeriesPage) -> StateHistoryPage {
        StateHistoryPage {
            reference: page.reference,
            range: page.range,
            snapshot: page.snapshot,
            observations: page
                .samples
                .into_iter()
                .map(|sample| StateObservation {
                    observed_at: sample.observed_at,
                    recorded_at: sample.recorded_at,
                    value: match sample.value {
                        DataTimeSeriesValue::Null => StateValue::Null,
                        DataTimeSeriesValue::Boolean(value) => StateValue::Boolean(value),
                        DataTimeSeriesValue::Integer(value) => StateValue::Integer(value),
                        DataTimeSeriesValue::Number(value) => StateValue::Number(value),
                        DataTimeSeriesValue::Text(value) => StateValue::Text(value),
                    },
                    quality: sample.quality,
                    provenance: sample.provenance,
                    attributes: sample.attributes,
                })
                .collect(),
            source_generation: page.source_generation,
            complete: !page.truncated,
            lineage: page.lineage,
            warnings: page.warnings,
        }
    }
}

pub use timeseries_adapter::DataTimeSeriesStateHistoryAdapter;

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex};

    use async_trait::async_trait;
    use chrono::{Duration, Utc};
    use univers_aip_contracts_data::core::{DataProvenance, TimeResolution};
    use univers_aip_contracts_data::timeseries::{
        DataTimeSeriesCapability, DataTimeSeriesPage, DataTimeSeriesQuery, DataTimeSeriesQueryPort,
        DataTimeSeriesSample, DataTimeSeriesSourcePolicy, DataTimeSeriesValue,
    };
    use univers_aip_contracts_world::state::{StateHistoryReadPort, StateValue};

    use super::{
        Arc, DataKind, DataPointResult, DataQuerySnapshot, DataRef,
        DataTimeSeriesStateHistoryAdapter, StateHistoryQuery, TimeRange,
    };

    struct RecordingPort(Mutex<Vec<DataTimeSeriesQuery>>);

    #[async_trait]
    impl DataTimeSeriesQueryPort for RecordingPort {
        fn query_capabilities(&self) -> Vec<DataTimeSeriesCapability> {
            vec![DataTimeSeriesCapability::StateQuery]
        }

        async fn query_batch(
            &self,
            queries: Vec<DataTimeSeriesQuery>,
        ) -> DataPointResult<Vec<DataTimeSeriesPage>> {
            self.0.lock().unwrap().clone_from(&queries);
            Ok(queries
                .iter()
                .map(|query| DataTimeSeriesPage {
                    reference: query.reference.clone(),
                    range: query.range.clone(),
                    snapshot: query.snapshot,
                    samples: vec![DataTimeSeriesSample {
                        observed_at: query.range.start + Duration::seconds(1),
                        recorded_at: query.snapshot.as_of,
                        value: DataTimeSeriesValue::Number(21.5),
                        quality: Some(90),
                        provenance: DataProvenance::Measured,
                        attributes: BTreeMap::new(),
                    }],
                    source_generation: Some(4),
                    provider_resolution: None,
                    truncated: false,
                    next_cursor: None,
                    lineage: Vec::new(),
                    warnings: Vec::new(),
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn adapter_keeps_physical_policy_behind_semantic_state_history() {
        let raw = Arc::new(RecordingPort(Mutex::new(Vec::new())));
        let adapter = DataTimeSeriesStateHistoryAdapter::new(raw.clone()).unwrap();
        let now = Utc::now();
        let query = StateHistoryQuery::new(
            DataRef::new("acme", DataKind::State, "temperature").unwrap(),
            TimeRange {
                start: now - Duration::hours(1),
                end: now + Duration::seconds(1),
            },
            DataQuerySnapshot { as_of: now },
        )
        .with_resolution(TimeResolution::Minutes(5))
        .unwrap();

        let page = adapter.read(query).await.unwrap();

        let physical = raw.0.lock().unwrap();
        assert_eq!(
            physical[0].source_policy,
            DataTimeSeriesSourcePolicy::LocalOnly
        );
        assert!(physical[0].aggregation.is_some());
        assert_eq!(page.source_generation, Some(4));
        assert_eq!(page.observations[0].value, StateValue::Number(21.5));
    }
}
