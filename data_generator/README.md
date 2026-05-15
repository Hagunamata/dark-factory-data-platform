# data_generator

Synthetic data generator that expands the two Kaggle reference datasets to 1,000,000+ rows while preserving their schemas and key statistical properties.

## Why this exists

The two Kaggle datasets used as schema references are small (~1k and ~80k rows). The course brief requires the pipeline to handle at least 1,000,000 data points. This module bridges the gap by generating realistic synthetic rows that:

- Match the column types, names, and value ranges of the source datasets
- Distribute timestamps realistically across an 18-month window with business-hour weighting
- Preserve the anomaly and optimisation label proportions present in the HRSS source data
- Include a controlled fraction of missing values and out-of-range readings

Full rationale is in `docs/01-conception.md` section 2.

## Module layout

```
data_generator/
├── README.md           # this file
├── Dockerfile          # container image used by docker-compose
├── requirements.txt    # python dependencies
├── __init__.py
├── schemas.py          # column definitions derived from the Kaggle CSVs
├── distributions.py    # statistical helpers (timestamps, value sampling)
├── generators.py       # row generation logic per source
└── bootstrap.py        # one-shot bulk generator for the initial historical load
```

## Usage

The generator runs inside the `data-generator` container defined in `docker-compose.yml`. Two entry points:

```bash
# One-shot bulk generation: produces the full 1M+ row historical dataset
# and pushes it to Kafka. Run once, after the stack is up.
docker compose exec data-generator python -m data_generator.bootstrap

# Continuous producer: emits new rows to Kafka at a configurable rate,
# simulating live factory telemetry. Run for demos.
docker compose exec data-generator python -m data_generator.continuous
```

Output rates and time windows are controlled by environment variables — see `.env.example` at the repository root.

## Design discipline

This is a **synthetic data generator**, not a research project on synthetic data quality. Statistical fidelity is "good enough to be defensible," not "indistinguishable from real data." Specifically:

- Numerical columns: empirical distributions sampled from the source CSV (with mild Gaussian jitter)
- Categorical columns: sampled with original frequencies
- Timestamps: piecewise uniform with business-hour density weighting
- Anomalies: injected at the proportion observed in the HRSS source
- No attempt to preserve multivariate correlations beyond per-column distributions

This is documented openly so the trade-off is visible in the portfolio.
