"""Schema definitions derived from the two Kaggle reference CSVs.

Each source column is summarised as a ColumnSchema carrying the values
observed in the source (after dropping nulls) plus a few derived stats:
the original null rate, and — for categoricals — the category frequencies.

generators.py then samples from these empirical pools (with mild jitter for
numerics) to produce synthetic rows that match the source's per-column
statistical character. No attempt is made to preserve multivariate
correlations; see data_generator/README.md "Design discipline".
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Literal, Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SAMPLE_DATA_DIR = Path(os.environ.get("SAMPLE_DATA_DIR", "/app/sample_data"))

LOGISTICS_FILE = "Smart Logistics Supply Chain Dataset.csv"
HRSS_FILES = {
    # (is_anomalous, is_optimised)
    (False, False): "HRSS_normal_standard.csv",
    (False, True): "HRSS_normal_optimized.csv",
    (True, False): "HRSS_anomalous_standard.csv",
    (True, True): "HRSS_anomalous_optimized.csv",
}

ColumnKind = Literal["integer", "numeric", "categorical", "boolean", "timestamp"]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ColumnSchema:
    name: str
    kind: ColumnKind
    values: np.ndarray                              # non-null observations
    null_rate: float = 0.0
    categories: Optional[Dict[str, float]] = None   # category -> frequency (categorical only)


@dataclass
class DatasetSchema:
    name: str
    columns: List[ColumnSchema] = field(default_factory=list)
    row_count: int = 0
    # For HRSS: per-source-file row counts, used to preserve original class proportions.
    source_class_weights: Optional[Dict[tuple, float]] = None

    def by_name(self, column_name: str) -> ColumnSchema:
        for col in self.columns:
            if col.name == column_name:
                return col
        raise KeyError(f"No column {column_name!r} in {self.name}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _infer_kind(series: pd.Series, force_categorical: bool = False) -> ColumnKind:
    if force_categorical:
        return "categorical"
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        return "numeric"
    return "categorical"


def _column_schema(series: pd.Series, kind: ColumnKind) -> ColumnSchema:
    non_null = series.dropna()
    null_rate = 1.0 - (len(non_null) / max(len(series), 1))
    categories = None
    if kind == "categorical":
        freq = non_null.value_counts(normalize=True)
        categories = {str(k): float(v) for k, v in freq.items()}
        values = non_null.astype(str).to_numpy()
    else:
        values = non_null.to_numpy()
    return ColumnSchema(
        name=str(series.name),
        kind=kind,
        values=values,
        null_rate=float(null_rate),
        categories=categories,
    )


# ---------------------------------------------------------------------------
# Smart Logistics
# ---------------------------------------------------------------------------

# Columns we treat as categorical regardless of dtype inference.
_LOGISTICS_CATEGORICAL = {
    "Asset_ID",
    "Shipment_Status",
    "Traffic_Status",
    "Logistics_Delay_Reason",
}


def load_logistics_schema() -> DatasetSchema:
    """Read the Smart Logistics CSV and derive a per-column schema."""
    path = SAMPLE_DATA_DIR / LOGISTICS_FILE
    df = pd.read_csv(path)

    schema = DatasetSchema(name="logistics_events", row_count=len(df))
    for col_name in df.columns:
        if col_name == "Timestamp":
            # Timestamps are regenerated per-row by distributions.business_hour_timestamps,
            # so we don't need to sample from the original values.
            continue
        kind = _infer_kind(df[col_name], force_categorical=col_name in _LOGISTICS_CATEGORICAL)
        schema.columns.append(_column_schema(df[col_name], kind))
    return schema


# ---------------------------------------------------------------------------
# HRSS — combine all four files, tag with is_anomalous / is_optimised
# ---------------------------------------------------------------------------

def load_hrss_schema() -> DatasetSchema:
    """Read the four HRSS CSVs, tag class flags, derive a per-column schema."""
    frames = []
    class_counts: Dict[tuple, int] = {}
    for (is_anomalous, is_optimised), fname in HRSS_FILES.items():
        path = SAMPLE_DATA_DIR / fname
        df = pd.read_csv(path)
        df["is_anomalous"] = is_anomalous
        df["is_optimised"] = is_optimised
        class_counts[(is_anomalous, is_optimised)] = len(df)
        frames.append(df)
    full = pd.concat(frames, ignore_index=True)
    total = sum(class_counts.values())
    class_weights = {k: v / total for k, v in class_counts.items()}

    schema = DatasetSchema(
        name="hrss_telemetry",
        row_count=len(full),
        source_class_weights=class_weights,
    )
    for col_name in full.columns:
        if col_name == "Timestamp":
            # Original Timestamp is cycle-elapsed seconds (float). We regenerate
            # both the real reading_timestamp and the cycle_elapsed_sec separately.
            continue
        if col_name in ("is_anomalous", "is_optimised"):
            # Class flags are emitted directly by the generator based on class_weights.
            continue
        # cycle_elapsed_sec is sampled from the original Timestamp distribution below.
        kind = _infer_kind(full[col_name])
        schema.columns.append(_column_schema(full[col_name], kind))

    # Sample pool for cycle_elapsed_sec (original float Timestamp).
    cycle_series = pd.concat([pd.read_csv(SAMPLE_DATA_DIR / fname)["Timestamp"]
                              for fname in HRSS_FILES.values()], ignore_index=True)
    schema.columns.append(
        ColumnSchema(
            name="cycle_elapsed_sec",
            kind="numeric",
            values=cycle_series.dropna().to_numpy(dtype=float),
            null_rate=0.0,
        )
    )
    return schema
