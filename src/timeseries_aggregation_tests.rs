//! Aggregation algorithm and provenance regression tests.

use std::collections::BTreeMap;

use chrono::{Duration, TimeZone, Utc};
use univers_aip_contracts_data::core::{DataProvenance, TimeRange, TimeResolution};

use crate::aggregate_time_series_samples;
use univers_aip_contracts_data::series_types::timeseries::{
    DataTimeSeriesAggregation, DataTimeSeriesAggregationFunction, DataTimeSeriesBucketAlignment,
    DataTimeSeriesSample, DataTimeSeriesValue,
};

fn aggregation_range() -> TimeRange {
    let start = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    TimeRange::new(start, start + Duration::hours(1)).unwrap()
}

fn aggregation_sample(minute: i64, value: f64, quality: Option<u8>) -> DataTimeSeriesSample {
    let observed_at = Utc.timestamp_opt(1_700_000_000, 0).unwrap() + Duration::minutes(minute);
    DataTimeSeriesSample {
        observed_at,
        recorded_at: observed_at,
        value: DataTimeSeriesValue::Number(value),
        quality,
        provenance: DataProvenance::Derived,
        attributes: BTreeMap::new(),
    }
}

#[test]
fn aggregation_buckets_samples_and_preserves_worst_quality() {
    let range = aggregation_range();
    let aggregation = DataTimeSeriesAggregation::new(
        TimeResolution::Minutes(5),
        DataTimeSeriesAggregationFunction::Mean,
    )
    .expect("aggregation is valid")
    .with_alignment(DataTimeSeriesBucketAlignment::RangeStart);

    let samples = aggregate_time_series_samples(
        &range,
        aggregation,
        vec![
            aggregation_sample(0, 10.0, Some(100)),
            aggregation_sample(1, 20.0, Some(80)),
            aggregation_sample(6, 30.0, Some(90)),
        ],
    )
    .expect("aggregation should succeed");

    assert_eq!(samples.len(), 2, "two five-minute buckets are expected");
    assert_eq!(samples[0].value, DataTimeSeriesValue::Number(15.0));
    assert_eq!(
        samples[0].quality,
        Some(80),
        "the worst observed quality must survive aggregation"
    );
    assert_eq!(samples[1].value, DataTimeSeriesValue::Number(30.0));
}

#[test]
fn aggregation_rejects_non_numeric_values_for_numeric_functions() {
    let range = aggregation_range();
    let aggregation = DataTimeSeriesAggregation::new(
        TimeResolution::Minutes(5),
        DataTimeSeriesAggregationFunction::Sum,
    )
    .expect("aggregation is valid")
    .with_alignment(DataTimeSeriesBucketAlignment::RangeStart);

    let mut sample = aggregation_sample(0, 0.0, None);
    sample.value = DataTimeSeriesValue::Boolean(true);
    assert!(
        aggregate_time_series_samples(&range, aggregation, vec![sample]).is_err(),
        "a numeric aggregation over a non-numeric sample must fail"
    );
}

#[test]
fn aggregation_keeps_missing_quality_and_marks_outputs_derived() {
    let range = aggregation_range();
    let aggregation = DataTimeSeriesAggregation::new(
        TimeResolution::Minutes(5),
        DataTimeSeriesAggregationFunction::Mean,
    )
    .unwrap();
    let mut first = aggregation_sample(0, 10.0, Some(100));
    first.provenance = DataProvenance::Measured;
    first.recorded_at = range.start + Duration::hours(2);
    first
        .attributes
        .insert("sensor".into(), "raw-source".into());
    let mut last = aggregation_sample(1, 20.0, None);
    last.provenance = DataProvenance::Measured;
    let result = aggregate_time_series_samples(&range, aggregation, vec![first, last]).unwrap();
    assert_eq!(result.len(), 1);
    let sample = &result[0];
    assert_eq!(sample.value, DataTimeSeriesValue::Number(15.0));
    assert_eq!(sample.quality, None);
    assert_eq!(sample.recorded_at, range.start + Duration::hours(2));
    assert_eq!(sample.provenance, DataProvenance::Derived);
    assert_eq!(
        sample.attributes,
        BTreeMap::from([
            ("aggregation.function".into(), "mean".into()),
            ("aggregation.samples".into(), "2".into()),
        ])
    );
}

#[test]
fn aggregation_aligns_pre_epoch_samples_with_euclidean_buckets() {
    let start = Utc.timestamp_opt(-120, 0).unwrap();
    let range = TimeRange::new(start, start + Duration::minutes(5)).unwrap();
    let samples = [-1, -61, 0]
        .into_iter()
        .map(|second| {
            let mut sample = aggregation_sample(0, 1.0, Some(100));
            sample.observed_at = Utc.timestamp_opt(second, 0).unwrap();
            sample.recorded_at = sample.observed_at;
            sample
        })
        .collect::<Vec<_>>();
    let aggregation = DataTimeSeriesAggregation::new(
        TimeResolution::Minutes(1),
        DataTimeSeriesAggregationFunction::Count,
    )
    .unwrap()
    .with_alignment(DataTimeSeriesBucketAlignment::UnixEpoch);
    let result = aggregate_time_series_samples(&range, aggregation, samples).unwrap();
    assert_eq!(
        result
            .iter()
            .map(|s| s.observed_at.timestamp())
            .collect::<Vec<_>>(),
        vec![-120, -60, 0]
    );
    assert!(result
        .iter()
        .all(|s| s.value == DataTimeSeriesValue::Integer(1)));
}

#[test]
fn first_and_last_preserve_input_order_within_each_bucket() {
    let range = aggregation_range();
    let mut first = aggregation_sample(2, 0.0, None);
    first.value = DataTimeSeriesValue::Text("first supplied".into());
    let mut last = aggregation_sample(1, 0.0, None);
    last.value = DataTimeSeriesValue::Boolean(true);
    for (function, expected) in [
        (
            DataTimeSeriesAggregationFunction::First,
            first.value.clone(),
        ),
        (DataTimeSeriesAggregationFunction::Last, last.value.clone()),
    ] {
        let aggregation =
            DataTimeSeriesAggregation::new(TimeResolution::Minutes(5), function).unwrap();
        let result =
            aggregate_time_series_samples(&range, aggregation, vec![first.clone(), last.clone()])
                .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, expected);
    }
}
