#![cfg(feature = "state-history-adapter")]

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use univers_aip_contracts_data::{
    core::{DataKind, DataPointResult, DataProvenance, DataQuerySnapshot, DataRef, TimeRange},
    timeseries::{
        DataTimeSeriesCapability, DataTimeSeriesPage, DataTimeSeriesQuery, DataTimeSeriesQueryPort,
        DataTimeSeriesSample, DataTimeSeriesValue,
    },
};
use univers_aip_contracts_world::state::{StateHistoryQuery, StateHistoryReadPort, StateValue};
use univers_aip_lib_timeseries::DataTimeSeriesStateHistoryAdapter;

struct Port {
    capable: bool,
    wrong_scope: bool,
    empty: bool,
}

#[async_trait]
impl DataTimeSeriesQueryPort for Port {
    fn query_capabilities(&self) -> Vec<DataTimeSeriesCapability> {
        if self.capable {
            vec![DataTimeSeriesCapability::StateQuery]
        } else {
            Vec::new()
        }
    }

    async fn query_batch(
        &self,
        queries: Vec<DataTimeSeriesQuery>,
    ) -> DataPointResult<Vec<DataTimeSeriesPage>> {
        Ok(queries
            .into_iter()
            .map(|query| DataTimeSeriesPage {
                reference: if self.wrong_scope {
                    DataRef::new("different-organization", DataKind::State, "temperature").unwrap()
                } else {
                    query.reference.clone()
                },
                range: query.range.clone(),
                snapshot: query.snapshot,
                samples: if self.empty {
                    Vec::new()
                } else {
                    vec![DataTimeSeriesSample {
                        observed_at: query.range.start + Duration::seconds(1),
                        recorded_at: query.snapshot.as_of,
                        value: DataTimeSeriesValue::Null,
                        quality: None,
                        provenance: DataProvenance::Derived,
                        attributes: BTreeMap::new(),
                    }]
                },
                source_generation: Some(4),
                provider_resolution: None,
                truncated: false,
                next_cursor: None,
                lineage: Vec::new(),
                warnings: vec!["source warning".to_owned()],
            })
            .collect())
    }
}

fn query() -> StateHistoryQuery {
    let now = Utc::now();
    StateHistoryQuery::new(
        DataRef::new("acme", DataKind::State, "temperature").unwrap(),
        TimeRange {
            start: now - Duration::hours(1),
            end: now + Duration::seconds(1),
        },
        DataQuerySnapshot { as_of: now },
    )
}

#[test]
fn state_capability_is_required() {
    assert!(DataTimeSeriesStateHistoryAdapter::new(Arc::new(Port {
        capable: false,
        wrong_scope: false,
        empty: false,
    }))
    .is_err());
}

#[tokio::test]
async fn source_response_must_match_requested_scope() {
    let adapter = DataTimeSeriesStateHistoryAdapter::new(Arc::new(Port {
        capable: true,
        wrong_scope: true,
        empty: false,
    }))
    .unwrap();
    assert!(adapter.read(query()).await.is_err());
}

#[tokio::test]
async fn missing_quality_and_null_values_are_not_filled_in() {
    let adapter = DataTimeSeriesStateHistoryAdapter::new(Arc::new(Port {
        capable: true,
        wrong_scope: false,
        empty: false,
    }))
    .unwrap();
    let page = adapter.read(query()).await.unwrap();
    assert_eq!(page.observations[0].value, StateValue::Null);
    assert_eq!(page.observations[0].quality, None);
    assert_eq!(page.observations[0].provenance, DataProvenance::Derived);
    assert_eq!(page.warnings, vec!["source warning"]);
    assert_eq!(page.source_generation, Some(4));
}

#[tokio::test]
async fn empty_source_remains_an_empty_state_history() {
    let adapter = DataTimeSeriesStateHistoryAdapter::new(Arc::new(Port {
        capable: true,
        wrong_scope: false,
        empty: true,
    }))
    .unwrap();
    let page = adapter.read(query()).await.unwrap();
    assert!(page.observations.is_empty());
    assert!(page.complete);
}
