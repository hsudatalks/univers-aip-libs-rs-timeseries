//! In-memory reference implementation of the canonical time-series Port.
//!
//! The store favors explicit semantics over throughput. It retains committed
//! write generations so historical snapshots, replace operations, retries, and
//! tenant isolation can be tested without a storage backend.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use univers_aip_contracts_data::core::{
    DataKind, DataPointError, DataPointResult, DataProvenance, DataRef, DataWriteOutcome,
    DataWriteReceipt, DataWriteResult, TimeRange,
};
use univers_aip_contracts_data::timeseries::{
    validate_time_series_batch, DataTimeSeriesAggregation, DataTimeSeriesCapability,
    DataTimeSeriesDurability, DataTimeSeriesHead, DataTimeSeriesPage, DataTimeSeriesQuery,
    DataTimeSeriesQueryPort, DataTimeSeriesSample, DataTimeSeriesWriteMode,
    DataTimeSeriesWritePort, DataTimeSeriesWriteRequest, DataTimeSeriesWriteResult,
    DataTimeSeriesWriteSample,
};

use crate::aggregate_time_series_samples;
#[cfg(test)]
use univers_aip_contracts_data::timeseries::{
    DataTimeSeriesAggregationFunction, DataTimeSeriesPort, DataTimeSeriesValue,
};

#[derive(Clone)]
struct CommittedWrite {
    generation: u64,
    recorded_at: DateTime<Utc>,
    mode: DataTimeSeriesWriteMode,
    provenance: DataProvenance,
    samples: Vec<DataTimeSeriesWriteSample>,
}

#[derive(Clone)]
struct CommittedReceipt {
    request: DataTimeSeriesWriteRequest,
    result: DataWriteResult<DataTimeSeriesWriteResult>,
}

#[derive(Default)]
struct StoreState {
    series: HashMap<DataRef, Vec<CommittedWrite>>,
    receipts: HashMap<(String, String), CommittedReceipt>,
    last_recorded_at: Option<DateTime<Utc>>,
}

impl StoreState {
    fn next_recorded_at(&mut self) -> DateTime<Utc> {
        let now = Utc::now();
        let recorded_at = self
            .last_recorded_at
            .map_or(now, |previous| now.max(previous + Duration::nanoseconds(1)));
        self.last_recorded_at = Some(recorded_at);
        recorded_at
    }
}

/// Executable reference semantics for canonical time-series reads and writes.
#[derive(Default)]
pub struct InMemoryDataTimeSeriesStore {
    state: Mutex<StoreState>,
}

impl InMemoryDataTimeSeriesStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> DataPointResult<std::sync::MutexGuard<'_, StoreState>> {
        self.state
            .lock()
            .map_err(|_| DataPointError::Internal("time-series store lock is poisoned".to_string()))
    }

    fn page(
        state: &StoreState,
        query: &DataTimeSeriesQuery,
    ) -> DataPointResult<DataTimeSeriesPage> {
        query.validate()?;
        if query.reference.kind() == DataKind::State {
            return Err(DataPointError::NotSupported(
                "the in-memory reference store has no State calculation resolver".to_string(),
            ));
        }
        let mut visible = BTreeMap::new();
        let mut generation = None;
        if let Some(writes) = state.series.get(&query.reference) {
            for write in writes.iter().filter(|write| {
                write.recorded_at <= query.snapshot.as_of
                    && query
                        .source_generation
                        .is_none_or(|generation| write.generation <= generation)
            }) {
                if matches!(
                    write.mode,
                    DataTimeSeriesWriteMode::Replace | DataTimeSeriesWriteMode::Delete
                ) {
                    visible.clear();
                }
                for sample in &write.samples {
                    visible.insert(
                        sample.observed_at,
                        DataTimeSeriesSample {
                            observed_at: sample.observed_at,
                            recorded_at: write.recorded_at,
                            value: sample.value.clone(),
                            quality: sample.quality,
                            provenance: write.provenance.clone(),
                            attributes: sample.attributes.clone(),
                        },
                    );
                }
                generation = Some(write.generation);
            }
        }
        if let Some(requested) = query.source_generation {
            if generation != Some(requested) {
                return Err(DataPointError::NotFound(format!(
                    "Signal generation {requested} is not visible at snapshot {}",
                    query.snapshot.as_of.to_rfc3339()
                )));
            }
        }

        let samples = visible
            .into_values()
            .filter(|sample| query.range.contains(&sample.observed_at))
            .collect::<Vec<_>>();
        let samples = match query.aggregation {
            Some(aggregation) => aggregate_time_series_samples(&query.range, aggregation, samples)?,
            None => samples,
        };
        paginate(query, generation, samples)
    }
}

#[async_trait]
impl DataTimeSeriesQueryPort for InMemoryDataTimeSeriesStore {
    fn query_capabilities(&self) -> Vec<DataTimeSeriesCapability> {
        vec![DataTimeSeriesCapability::SignalQuery]
    }

    async fn query_batch(
        &self,
        queries: Vec<DataTimeSeriesQuery>,
    ) -> DataPointResult<Vec<DataTimeSeriesPage>> {
        validate_time_series_batch(&queries)?;
        let state = self.lock()?;
        queries
            .iter()
            .map(|query| Self::page(&state, query))
            .collect()
    }

    async fn head(&self, reference: &DataRef) -> DataPointResult<DataTimeSeriesHead> {
        if reference.kind() != DataKind::Signal || reference.version().is_some() {
            return Err(DataPointError::Validation(
                "time-series head requires an unversioned Signal reference".to_string(),
            ));
        }
        let state = self.lock()?;
        let latest = state.series.get(reference).and_then(|writes| writes.last());
        let head = DataTimeSeriesHead {
            reference: reference.clone(),
            generation: latest.map_or(0, |write| write.generation),
            last_recorded_at: latest.map(|write| write.recorded_at),
            pending: None,
        };
        head.validate_for(reference)?;
        Ok(head)
    }
}

#[async_trait]
impl DataTimeSeriesWritePort for InMemoryDataTimeSeriesStore {
    async fn write(
        &self,
        request: DataTimeSeriesWriteRequest,
    ) -> DataPointResult<DataWriteResult<DataTimeSeriesWriteResult>> {
        request.validate()?;
        let mut state = self.lock()?;
        let receipt_key = (
            request.reference.organization_id().to_string(),
            request.idempotency_key.clone(),
        );
        if let Some(committed) = state.receipts.get(&receipt_key) {
            if committed.request != request {
                return Err(DataPointError::AlreadyExists(
                    "time-series idempotency_key is already bound to a different payload"
                        .to_string(),
                ));
            }
            let mut replay = committed.result.clone();
            replay.receipt.outcome = DataWriteOutcome::Skipped;
            replay
                .receipt
                .warnings
                .push("idempotent replay returned the committed time-series write".to_string());
            replay.receipt.mark_replayed()?;
            DataTimeSeriesWriteResult::validate_for(&replay, &request)?;
            return Ok(replay);
        }

        let current_generation = state
            .series
            .get(&request.reference)
            .and_then(|writes| writes.last())
            .map_or(0, |write| write.generation);
        if let Some(expected) = request.expected_generation {
            if expected != current_generation {
                return Err(DataPointError::InvalidOperation(format!(
                    "time-series generation changed: expected {expected}, current {current_generation}"
                )));
            }
        }
        let generation = current_generation.checked_add(1).ok_or_else(|| {
            DataPointError::InvalidOperation("time-series generation overflow".to_string())
        })?;
        let recorded_at = state.next_recorded_at();
        let outcome = if request.mode == DataTimeSeriesWriteMode::Delete {
            DataWriteOutcome::Deleted
        } else if current_generation == 0 {
            DataWriteOutcome::Created
        } else if request.mode == DataTimeSeriesWriteMode::Replace {
            DataWriteOutcome::Replaced
        } else {
            DataWriteOutcome::Updated
        };
        state
            .series
            .entry(request.reference.clone())
            .or_default()
            .push(CommittedWrite {
                generation,
                recorded_at,
                mode: request.mode,
                provenance: request.provenance.clone(),
                samples: request.samples.clone(),
            });
        let result = DataWriteResult::new(
            DataTimeSeriesWriteResult {
                reference: request.reference.clone(),
                samples_written: request.samples.len(),
                generation,
                durability: if request.samples.is_empty() {
                    DataTimeSeriesDurability::NotApplicable
                } else {
                    DataTimeSeriesDurability::Volatile
                },
            },
            DataWriteReceipt::new(outcome, request.reference.clone(), recorded_at)
                .with_idempotency_key(request.idempotency_key.clone())?,
        );
        DataTimeSeriesWriteResult::validate_for(&result, &request)?;
        state.receipts.insert(
            receipt_key,
            CommittedReceipt {
                request,
                result: result.clone(),
            },
        );
        Ok(result)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageCursor {
    reference: DataRef,
    range: TimeRange,
    snapshot: univers_aip_contracts_data::core::DataQuerySnapshot,
    aggregation: Option<DataTimeSeriesAggregation>,
    offset: usize,
}

pub fn paginate(
    query: &DataTimeSeriesQuery,
    generation: Option<u64>,
    samples: Vec<DataTimeSeriesSample>,
) -> DataPointResult<DataTimeSeriesPage> {
    let offset = match query.cursor.as_deref() {
        Some(cursor) => {
            let cursor: PageCursor = serde_json::from_str(cursor).map_err(|_| {
                DataPointError::Validation("invalid time-series page cursor".to_string())
            })?;
            if cursor.reference != query.reference
                || cursor.range != query.range
                || cursor.snapshot != query.snapshot
                || cursor.aggregation != query.aggregation
            {
                return Err(DataPointError::Validation(
                    "time-series page cursor does not belong to this query".to_string(),
                ));
            }
            cursor.offset
        }
        None => 0,
    };
    if offset > samples.len() {
        return Err(DataPointError::Validation(
            "time-series page cursor is beyond the snapshot result".to_string(),
        ));
    }
    let end = offset.saturating_add(query.limit).min(samples.len());
    let truncated = end < samples.len();
    let next_cursor = truncated
        .then(|| {
            serde_json::to_string(&PageCursor {
                reference: query.reference.clone(),
                range: query.range.clone(),
                snapshot: query.snapshot,
                aggregation: query.aggregation,
                offset: end,
            })
            .map_err(|error| DataPointError::Serialization(error.to_string()))
        })
        .transpose()?;
    let page = DataTimeSeriesPage {
        reference: query.reference.clone(),
        range: query.range.clone(),
        snapshot: query.snapshot,
        samples: samples[offset..end].to_vec(),
        source_generation: generation,
        provider_resolution: None,
        truncated,
        next_cursor,
        lineage: Vec::new(),
        warnings: Vec::new(),
    };
    page.validate_for(query)?;
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_manifest_does_not_claim_state_queries() {
        let manifest = InMemoryDataTimeSeriesStore::new().capability_manifest();

        assert!(!manifest.complete);
        assert_eq!(
            manifest.missing,
            vec![
                DataTimeSeriesCapability::SignalStagedWrite,
                DataTimeSeriesCapability::StateQuery,
            ]
        );
    }
    use chrono::{TimeZone, Utc};
    use univers_aip_contracts_data::core::{DataKind, DataQuerySnapshot};

    fn reference(organization_id: &str) -> DataRef {
        DataRef::new(organization_id, DataKind::Signal, "temperature").unwrap()
    }

    fn request(
        organization_id: &str,
        idempotency_key: &str,
        mode: DataTimeSeriesWriteMode,
        values: &[(i64, f64)],
        expected_generation: Option<u64>,
    ) -> DataTimeSeriesWriteRequest {
        DataTimeSeriesWriteRequest {
            reference: reference(organization_id),
            mode,
            samples: values
                .iter()
                .map(|(second, value)| DataTimeSeriesWriteSample {
                    observed_at: Utc.timestamp_opt(*second, 0).unwrap(),
                    value: DataTimeSeriesValue::Number(*value),
                    quality: Some(100),
                    attributes: BTreeMap::new(),
                })
                .collect(),
            provenance: DataProvenance::Measured,
            idempotency_key: idempotency_key.to_string(),
            expected_generation,
        }
    }

    fn query(reference: DataRef, snapshot: DateTime<Utc>) -> DataTimeSeriesQuery {
        DataTimeSeriesQuery::new(
            reference,
            TimeRange::new(
                Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                Utc.timestamp_opt(1_700_001_000, 0).unwrap(),
            )
            .unwrap(),
            DataQuerySnapshot { as_of: snapshot },
        )
    }

    #[tokio::test]
    async fn writes_are_tenant_isolated_retry_safe_and_generation_guarded() {
        let store = InMemoryDataTimeSeriesStore::new();
        let org_a = request(
            "org-a",
            "write-1",
            DataTimeSeriesWriteMode::Append,
            &[(1_700_000_001, 21.0)],
            Some(0),
        );
        let org_b = request(
            "org-b",
            "write-1",
            DataTimeSeriesWriteMode::Append,
            &[(1_700_000_001, 42.0)],
            Some(0),
        );
        let first = store.write(org_a.clone()).await.unwrap();
        let replay = store.write(org_a.clone()).await.unwrap();
        let other = store.write(org_b).await.unwrap();

        assert_eq!(first.value.generation, 1);
        assert!(replay.receipt.replayed);
        assert_eq!(replay.receipt.outcome, DataWriteOutcome::Skipped);
        assert_eq!(replay.receipt.recorded_at, first.receipt.recorded_at);
        assert_eq!(other.value.generation, 1);

        let mut conflicting = org_a;
        conflicting.samples[0].value = DataTimeSeriesValue::Number(99.0);
        assert!(matches!(
            store.write(conflicting).await,
            Err(DataPointError::AlreadyExists(_))
        ));
        let stale = request(
            "org-a",
            "write-2",
            DataTimeSeriesWriteMode::Append,
            &[(1_700_000_002, 22.0)],
            Some(0),
        );
        assert!(matches!(
            store.write(stale).await,
            Err(DataPointError::InvalidOperation(_))
        ));
    }

    #[tokio::test]
    async fn replace_preserves_historical_snapshots() {
        let store = InMemoryDataTimeSeriesStore::new();
        let first = store
            .write(request(
                "org-a",
                "write-1",
                DataTimeSeriesWriteMode::Append,
                &[(1_700_000_001, 21.0)],
                Some(0),
            ))
            .await
            .unwrap();
        let second = store
            .write(request(
                "org-a",
                "write-2",
                DataTimeSeriesWriteMode::Replace,
                &[(1_700_000_002, 42.0)],
                Some(1),
            ))
            .await
            .unwrap();

        let before_replace = store
            .query(query(reference("org-a"), first.receipt.recorded_at))
            .await
            .unwrap();
        let after_replace = store
            .query(query(reference("org-a"), second.receipt.recorded_at))
            .await
            .unwrap();
        let exact_first = store
            .query(
                query(reference("org-a"), second.receipt.recorded_at)
                    .at_source_generation(1)
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(before_replace.source_generation, Some(1));
        assert_eq!(
            before_replace.samples[0].value,
            DataTimeSeriesValue::Number(21.0)
        );
        assert_eq!(after_replace.source_generation, Some(2));
        assert_eq!(after_replace.samples.len(), 1);
        assert_eq!(
            after_replace.samples[0].value,
            DataTimeSeriesValue::Number(42.0)
        );
        assert_eq!(exact_first.source_generation, Some(1));
        assert_eq!(
            exact_first.samples[0].value,
            DataTimeSeriesValue::Number(21.0)
        );
    }

    #[tokio::test]
    async fn pagination_and_aggregation_are_snapshot_stable() {
        let store = InMemoryDataTimeSeriesStore::new();
        let mut write = request(
            "org-a",
            "write-1",
            DataTimeSeriesWriteMode::Append,
            &[
                (1_700_000_001, 20.0),
                (1_700_000_002, 22.0),
                (1_700_000_011, 30.0),
            ],
            Some(0),
        );
        write.samples[1].quality = None;
        let written = store.write(write).await.unwrap();
        let aggregation = DataTimeSeriesAggregation::new(
            univers_aip_contracts_data::core::TimeResolution::Seconds(10),
            DataTimeSeriesAggregationFunction::Mean,
        )
        .unwrap();
        let first_query = query(reference("org-a"), written.receipt.recorded_at)
            .with_aggregation(aggregation)
            .unwrap()
            .with_limit(1)
            .unwrap();
        let first = store.query(first_query.clone()).await.unwrap();
        let second = store
            .query(
                first_query
                    .with_cursor(first.next_cursor.clone().unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(first.truncated);
        assert_eq!(first.samples[0].value, DataTimeSeriesValue::Number(21.0));
        assert_eq!(first.samples[0].quality, None);
        assert!(!second.truncated);
        assert_eq!(second.samples[0].value, DataTimeSeriesValue::Number(30.0));
        assert_eq!(second.samples[0].provenance, DataProvenance::Derived);
    }

    #[tokio::test]
    async fn unix_epoch_alignment_is_explicit_and_stable() {
        let store = InMemoryDataTimeSeriesStore::new();
        let written = store
            .write(request(
                "org-a",
                "write-alignment",
                DataTimeSeriesWriteMode::Append,
                &[(1_700_000_001, 20.0), (1_700_000_002, 22.0)],
                Some(0),
            ))
            .await
            .unwrap();
        let aggregation = DataTimeSeriesAggregation::new(
            univers_aip_contracts_data::core::TimeResolution::Seconds(10),
            DataTimeSeriesAggregationFunction::Mean,
        )
        .unwrap()
        .with_alignment(
            univers_aip_contracts_data::timeseries::DataTimeSeriesBucketAlignment::UnixEpoch,
        );
        let result = store
            .query(
                query(reference("org-a"), written.receipt.recorded_at)
                    .with_aggregation(aggregation)
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(result.samples[0].observed_at.timestamp(), 1_700_000_000);
        assert_eq!(result.samples[0].value, DataTimeSeriesValue::Number(21.0));
    }

    #[tokio::test]
    async fn state_queries_fail_explicitly_without_a_calculation_resolver() {
        let store = InMemoryDataTimeSeriesStore::new();
        let state = DataRef::new("org-a", DataKind::State, "temperature").unwrap();
        let result = store.query(query(state, Utc::now())).await;

        assert!(matches!(result, Err(DataPointError::NotSupported(_))));
    }

    #[tokio::test]
    async fn delete_is_a_snapshot_preserving_generation() {
        let store = InMemoryDataTimeSeriesStore::new();
        let written = store
            .write(request(
                "org-a",
                "write-before-delete",
                DataTimeSeriesWriteMode::Append,
                &[(1_700_000_001, 21.0)],
                None,
            ))
            .await
            .unwrap();
        let deleted = store
            .write(DataTimeSeriesWriteRequest {
                reference: reference("org-a"),
                mode: DataTimeSeriesWriteMode::Delete,
                samples: Vec::new(),
                provenance: DataProvenance::Manual,
                idempotency_key: "delete-series".to_string(),
                expected_generation: Some(1),
            })
            .await
            .unwrap();
        assert_eq!(deleted.receipt.outcome, DataWriteOutcome::Deleted);
        assert_eq!(deleted.value.generation, 2);
        assert!(store
            .query(query(reference("org-a"), deleted.receipt.recorded_at))
            .await
            .unwrap()
            .samples
            .is_empty());
        assert_eq!(
            store
                .query(query(reference("org-a"), written.receipt.recorded_at))
                .await
                .unwrap()
                .samples
                .len(),
            1
        );
    }
}
