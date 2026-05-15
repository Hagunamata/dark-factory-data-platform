"""Statistical helpers for synthetic data generation.

Vectorised on numpy arrays — generating 1M rows row-by-row in Python is
too slow. Each function takes a numpy Generator so the bootstrap run is
reproducible from a single seed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import numpy as np

from .schemas import ColumnSchema


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------

# Hour-of-day weights for a 24h dark-factory operation:
# slight dip overnight (lights-out maintenance windows, lower throughput),
# heavier weight during day shifts. Chosen to be "plausible" — see
# data_generator/README.md "Design discipline".
_HOUR_OF_DAY_WEIGHTS = np.array([
    0.6, 0.5, 0.5, 0.5, 0.6, 0.8,   # 00-05
    1.0, 1.2, 1.4, 1.5, 1.5, 1.4,   # 06-11
    1.3, 1.4, 1.5, 1.5, 1.4, 1.3,   # 12-17
    1.2, 1.1, 1.0, 0.9, 0.8, 0.7,   # 18-23
])
_HOUR_OF_DAY_WEIGHTS = _HOUR_OF_DAY_WEIGHTS / _HOUR_OF_DAY_WEIGHTS.sum()


def business_hour_timestamps(
    rng: np.random.Generator,
    n: int,
    start: datetime,
    end: datetime,
) -> np.ndarray:
    """Generate `n` UTC timestamps within [start, end], weighted by hour of day.

    Returns a numpy array of np.datetime64[s] sorted ascending — sorted
    timestamps match how a real ingestion path sees events from disk replay.
    """
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    start_s = int(start.timestamp())
    end_s = int(end.timestamp())
    span_s = end_s - start_s
    if span_s <= 0:
        raise ValueError("end must be strictly after start")

    # Pick day offset uniformly across the window, then pick hour by weight,
    # then pick minute/second uniformly within the hour.
    total_days = max(span_s // 86400, 1)
    day_offsets = rng.integers(0, total_days, size=n)
    hours = rng.choice(24, size=n, p=_HOUR_OF_DAY_WEIGHTS)
    minute_seconds = rng.integers(0, 3600, size=n)

    seconds = start_s + day_offsets * 86400 + hours * 3600 + minute_seconds
    seconds = np.clip(seconds, start_s, end_s)
    seconds.sort()
    return seconds.astype("datetime64[s]")


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def sample_empirical(
    rng: np.random.Generator,
    column: ColumnSchema,
    n: int,
    jitter: float = 0.02,
) -> np.ndarray:
    """Bootstrap-sample `n` values from a ColumnSchema.

    For numeric/integer columns, applies multiplicative Gaussian jitter of
    `jitter` (relative). For categorical/boolean columns, samples by
    observed frequency.
    """
    if column.kind == "categorical":
        if column.categories:
            cats = list(column.categories.keys())
            probs = np.array(list(column.categories.values()))
            probs = probs / probs.sum()
            return rng.choice(cats, size=n, p=probs)
        return rng.choice(column.values, size=n)

    if column.kind == "boolean":
        # Treat as 0/1 categorical.
        vals = column.values.astype(bool)
        p_true = float(vals.mean()) if len(vals) else 0.5
        return rng.random(size=n) < p_true

    values = column.values
    if len(values) == 0:
        return np.zeros(n)

    idx = rng.integers(0, len(values), size=n)
    sampled = values[idx].astype(float)

    if jitter > 0:
        noise = 1.0 + rng.normal(0.0, jitter, size=n)
        sampled = sampled * noise

    if column.kind == "integer":
        return np.rint(sampled).astype(np.int64)
    return sampled


# ---------------------------------------------------------------------------
# Noise injection (missing values, outliers)
# ---------------------------------------------------------------------------

def inject_missing(
    rng: np.random.Generator,
    arr: np.ndarray,
    rate: float,
    placeholder=None,
) -> np.ndarray:
    """Replace a `rate` fraction of `arr` with `placeholder` (None / NaN)."""
    if rate <= 0 or len(arr) == 0:
        return arr
    mask = rng.random(size=len(arr)) < rate
    if not mask.any():
        return arr
    out = arr.astype(object) if placeholder is None else arr.copy()
    out[mask] = placeholder
    return out


def inject_outliers(
    rng: np.random.Generator,
    arr: np.ndarray,
    rate: float,
    magnitude: float = 3.0,
) -> np.ndarray:
    """Push a `rate` fraction of numeric `arr` `magnitude`× further from the median.

    Used to simulate sensor anomalies on numeric columns.
    """
    if rate <= 0 or len(arr) == 0 or not np.issubdtype(arr.dtype, np.number):
        return arr
    mask = rng.random(size=len(arr)) < rate
    if not mask.any():
        return arr
    out = arr.copy()
    median = float(np.median(arr))
    deviation = arr[mask] - median
    out[mask] = median + deviation * magnitude
    return out
