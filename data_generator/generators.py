"""Batch row generators for the two source datasets.

These return lists of plain dicts ready to be JSON-serialised and sent to
Kafka. Field names match the raw Postgres table columns defined in
postgres/init/01_schemas.sql so the downstream consumer can insert verbatim.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List

import numpy as np

from .distributions import (
    business_hour_timestamps,
    inject_missing,
    sample_empirical,
)
from .schemas import DatasetSchema


# ---------------------------------------------------------------------------
# Smart Logistics → raw.logistics_events
# ---------------------------------------------------------------------------

# Source column name -> raw.logistics_events column name.
_LOGISTICS_COLUMN_MAP = {
    "Asset_ID":                "asset_id",
    "Latitude":                "latitude",
    "Longitude":               "longitude",
    "Inventory_Level":         "inventory_level",
    "Shipment_Status":         "shipment_status",
    "Temperature":             "temperature_c",
    "Humidity":                "humidity_pct",
    "Traffic_Status":          "traffic_status",
    "Waiting_Time":            "waiting_time_min",
    "User_Transaction_Amount": "user_transaction_amount",
    "User_Purchase_Frequency": "user_purchase_frequency",
    "Logistics_Delay_Reason":  "logistics_delay_reason",
    "Asset_Utilization":       "asset_utilization_pct",
    "Demand_Forecast":         "demand_forecast",
    "Logistics_Delay":         "logistics_delay",
}

_LOGISTICS_MISSING_RATE = 0.005  # mild — real sensors drop ~0.5% in our model


def generate_logistics_batch(
    rng: np.random.Generator,
    schema: DatasetSchema,
    n: int,
    window_start: datetime,
    window_end: datetime,
) -> List[Dict]:
    timestamps = business_hour_timestamps(rng, n, window_start, window_end)

    columns: Dict[str, np.ndarray] = {}
    for src_col, dst_col in _LOGISTICS_COLUMN_MAP.items():
        col_schema = schema.by_name(src_col)
        arr = sample_empirical(rng, col_schema, n)
        # The logistics dataset has no real nulls, but introduce a small rate
        # to make the pipeline exercise NULL handling.
        if col_schema.kind in ("numeric", "integer"):
            arr = inject_missing(rng, arr, _LOGISTICS_MISSING_RATE, placeholder=None)
        columns[dst_col] = arr

    # The source has Logistics_Delay as 0/1 int; the raw schema expects boolean.
    columns["logistics_delay"] = np.array(
        [bool(v) if v is not None else None for v in columns["logistics_delay"]],
        dtype=object,
    )

    rows: List[Dict] = []
    for i in range(n):
        row = {"event_timestamp": str(timestamps[i]) + "Z"}
        for dst_col, arr in columns.items():
            row[dst_col] = _py(arr[i])
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# HRSS → raw.hrss_telemetry
# ---------------------------------------------------------------------------

# Source column name -> raw.hrss_telemetry column name.
_HRSS_COLUMN_MAP = {
    "Labels":          "label",
    "I_w_BLO_Weg":     "blo_position_mm",
    "O_w_BLO_power":   "blo_power_w",
    "O_w_BLO_voltage": "blo_voltage_v",
    "I_w_BHL_Weg":     "bhl_position_mm",
    "O_w_BHL_power":   "bhl_power_w",
    "O_w_BHL_voltage": "bhl_voltage_v",
    "I_w_BHR_Weg":     "bhr_position_mm",
    "O_w_BHR_power":   "bhr_power_w",
    "O_w_BHR_voltage": "bhr_voltage_v",
    "I_w_BRU_Weg":     "bru_position_mm",
    "O_w_BRU_power":   "bru_power_w",
    "O_w_BRU_voltage": "bru_voltage_v",
    "I_w_HR_Weg":      "hr_position_mm",
    "O_w_HR_power":    "hr_power_w",
    "O_w_HR_voltage":  "hr_voltage_v",
    "I_w_HL_Weg":      "hl_position_mm",
    "O_w_HL_power":    "hl_power_w",
    "O_w_HL_voltage":  "hl_voltage_v",
}


def generate_hrss_batch(
    rng: np.random.Generator,
    schema: DatasetSchema,
    n: int,
    window_start: datetime,
    window_end: datetime,
) -> List[Dict]:
    timestamps = business_hour_timestamps(rng, n, window_start, window_end)

    columns: Dict[str, np.ndarray] = {}
    for src_col, dst_col in _HRSS_COLUMN_MAP.items():
        col_schema = schema.by_name(src_col)
        columns[dst_col] = sample_empirical(rng, col_schema, n)

    columns["cycle_elapsed_sec"] = sample_empirical(
        rng, schema.by_name("cycle_elapsed_sec"), n, jitter=0.0
    )

    # Sample is_anomalous / is_optimised from the original four-class proportions.
    weights = schema.source_class_weights or {}
    if weights:
        classes = list(weights.keys())          # list of (is_anomalous, is_optimised)
        probs = np.array(list(weights.values()))
        probs = probs / probs.sum()
        choices = rng.choice(len(classes), size=n, p=probs)
        is_anomalous = np.array([classes[i][0] for i in choices], dtype=bool)
        is_optimised = np.array([classes[i][1] for i in choices], dtype=bool)
    else:
        is_anomalous = rng.random(size=n) < 0.5
        is_optimised = rng.random(size=n) < 0.5

    columns["is_anomalous"] = is_anomalous
    columns["is_optimised"] = is_optimised

    rows: List[Dict] = []
    for i in range(n):
        row = {"reading_timestamp": str(timestamps[i]) + "Z"}
        for dst_col, arr in columns.items():
            row[dst_col] = _py(arr[i])
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _py(value):
    """Coerce numpy scalars to plain Python so json.dumps doesn't choke."""
    if value is None:
        return None
    if isinstance(value, (np.floating,)):
        f = float(value)
        return None if np.isnan(f) else f
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value
