"""Schema definitions for synthetic data generation.

This module reads the two Kaggle reference CSVs in sample_data/ and derives:
  - Column names and dtypes
  - Empirical value distributions for numeric columns
  - Category frequencies for categorical columns
  - Null rate per column

The derived schemas are then used by generators.py to produce synthetic rows
that match the source datasets' characteristics.

TODO (Phase 2):
  - Implement `load_logistics_schema()` reading sample_data/smart_logistics.csv
  - Implement `load_hrss_schema()` reading the four HRSS files and merging
    them with `is_anomalous` and `is_optimised` flag columns
  - Expose ColumnSchema and DatasetSchema dataclasses with the metadata above
"""
