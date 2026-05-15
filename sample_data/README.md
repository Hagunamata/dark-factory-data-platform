# sample_data/

This directory holds the small Kaggle reference CSVs used as schema sources by the synthetic data generator.

## Files to place here

> [!IMPORTANT]
> These CSVs are not committed to git directly because of their licenses. Download them yourself from Kaggle and drop them here.

| File | Source | Used for |
|---|---|---|
| `smart_logistics.csv` | https://www.kaggle.com/datasets/ziya07/smart-logistics-supply-chain-dataset | Schema for the logistics events source (~1k rows) |
| `HRSS_normal_standard.csv` | https://www.kaggle.com/datasets/inIT-OWL/high-storage-system-data-for-energy-optimization | Schema for HRSS telemetry, normal + standard operation |
| `HRSS_normal_optimized.csv` | same | HRSS telemetry, normal + optimised operation |
| `HRSS_anomalous_standard.csv` | same | HRSS telemetry, anomalous + standard operation |
| `HRSS_anomalous_optimized.csv` | same | HRSS telemetry, anomalous + optimised operation |

## What happens to these files

- They are read **once** by `data_generator/schemas.py` to derive column names, types, and statistical distributions
- They are **never** ingested directly into the pipeline — all data flowing through Kafka is synthetic, generated to match these schemas at scale

## Licenses

Both datasets are subject to the licenses on their Kaggle pages. They are used here for **educational purposes only** as schema references. Generated data derived from their schemas does not contain any original rows.
