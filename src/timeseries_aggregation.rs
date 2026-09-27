//! Fixed-width aggregation over already-materialized Data samples.
//!
//! This shared Data helper owns the algorithm. Contracts retain only aggregation and
//! sample values; no provider, clock, executor, or storage is required here.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use univers_aip_contracts_data::core::{
    DataPointError, DataPointResult, DataProvenance, TimeRange,
};

use univers_aip_contracts_data::series_types::timeseries::{
    DataTimeSeriesAggregation, DataTimeSeriesAggregationFunction, DataTimeSeriesBucketAlignment,
    DataTimeSeriesSample, DataTimeSeriesValue,
};

/// Reference fixed-width aggregation for in-process Data time-series adapters.
pub fn aggregate_time_series_samples(
    range: &TimeRange,
    aggregation: DataTimeSeriesAggregation,
    samples: Vec<DataTimeSeriesSample>,
) -> DataPointResult<Vec<DataTimeSeriesSample>> {
    aggregation.validate()?;
    let width_ms = aggregation.resolution.to_duration().num_milliseconds();
    let anchor_ms = match aggregation.alignment {
        DataTimeSeriesBucketAlignment::RangeStart => range.start.timestamp_millis(),
        DataTimeSeriesBucketAlignment::UnixEpoch => 0,
    };
    let mut buckets: BTreeMap<i64, Vec<DataTimeSeriesSample>> = BTreeMap::new();
    for sample in samples {
        let offset = sample.observed_at.timestamp_millis() - anchor_ms;
        let bucket = anchor_ms + offset.div_euclid(width_ms) * width_ms;
        buckets.entry(bucket).or_default().push(sample);
    }
    buckets
        .into_iter()
        .map(|(bucket, samples)| {
            aggregate_time_series_bucket(bucket, aggregation.function, samples)
        })
        .collect()
}

fn aggregate_time_series_bucket(
    bucket_ms: i64,
    function: DataTimeSeriesAggregationFunction,
    samples: Vec<DataTimeSeriesSample>,
) -> DataPointResult<DataTimeSeriesSample> {
    let first = samples.first().expect("aggregation bucket is non-empty");
    let last = samples.last().expect("aggregation bucket is non-empty");
    let value = match function {
        DataTimeSeriesAggregationFunction::First => first.value.clone(),
        DataTimeSeriesAggregationFunction::Last => last.value.clone(),
        DataTimeSeriesAggregationFunction::Count => {
            DataTimeSeriesValue::Integer(i64::try_from(samples.len()).unwrap_or(i64::MAX))
        }
        numeric => {
            let values = samples
                .iter()
                .map(|sample| match sample.value {
                    DataTimeSeriesValue::Integer(value) => Ok(value as f64),
                    DataTimeSeriesValue::Number(value) => Ok(value),
                    _ => Err(DataPointError::Validation(format!(
                        "{numeric:?} aggregation requires numeric time-series values"
                    ))),
                })
                .collect::<DataPointResult<Vec<_>>>()?;
            let value = match numeric {
                DataTimeSeriesAggregationFunction::Mean => {
                    values.iter().sum::<f64>() / values.len() as f64
                }
                DataTimeSeriesAggregationFunction::Sum => values.iter().sum(),
                DataTimeSeriesAggregationFunction::Min => {
                    values.iter().copied().fold(f64::INFINITY, f64::min)
                }
                DataTimeSeriesAggregationFunction::Max => {
                    values.iter().copied().fold(f64::NEG_INFINITY, f64::max)
                }
                other => {
                    return Err(DataPointError::Validation(format!(
                        "{other:?} aggregation is not supported for numeric buckets"
                    )));
                }
            };
            DataTimeSeriesValue::Number(value)
        }
    };
    let quality = samples.iter().try_fold(u8::MAX, |quality, sample| {
        sample
            .quality
            .map(|sample_quality| quality.min(sample_quality))
    });
    let recorded_at = samples
        .iter()
        .map(|sample| sample.recorded_at)
        .max()
        .expect("aggregation bucket is non-empty");
    let mut attributes = BTreeMap::new();
    attributes.insert(
        "aggregation.function".to_string(),
        format!("{function:?}").to_lowercase(),
    );
    attributes.insert("aggregation.samples".to_string(), samples.len().to_string());
    Ok(DataTimeSeriesSample {
        observed_at: DateTime::<Utc>::from_timestamp_millis(bucket_ms).ok_or_else(|| {
            DataPointError::Validation("aggregation bucket timestamp is invalid".to_string())
        })?,
        recorded_at,
        value,
        quality,
        provenance: DataProvenance::Derived,
        attributes,
    })
}
