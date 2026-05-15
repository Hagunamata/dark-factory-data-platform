"""Statistical helpers for synthetic data generation.

TODO (Phase 2):
  - business_hour_timestamps(n, start, end) — generate timestamps with
    realistic shift-pattern density (heavier during work hours, lighter at night)
  - sample_from_empirical(column_values, n) — bootstrap-style resampling
    with mild Gaussian jitter for numeric columns
  - inject_missing(series, rate) — replace a fraction of values with NaN
  - inject_outliers(series, rate, magnitude) — push a fraction to plausible
    out-of-range values to mimic sensor anomalies
"""
