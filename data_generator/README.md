# data_generator

Generates ~1 000 000 synthetic rows from the two Kaggle reference datasets and pushes them to Kafka. Used to seed the pipeline before the first ingest.

## Files

```
schemas.py         column definitions derived from the Kaggle CSVs
distributions.py   timestamp weighting and empirical sampling helpers
generators.py      batch row generators (list[dict] per source)
bootstrap.py       one-shot: seed both topics with the configured row counts
```

## Usage

```bash
docker compose exec data-generator python -m data_generator.bootstrap
```

Row counts, time window, and Kafka topic names are read from environment variables — see `.env.example`.

## What the generator does

- Reads the Kaggle CSVs in `sample_data/` and derives per-column empirical distributions (values, null rate, category frequencies).
- Draws samples with mild Gaussian jitter for numeric columns; samples categoricals by their observed frequency.
- Distributes timestamps across the configured window with an hour-of-day weighting profile.
- For HRSS, preserves the class proportions of the four source files (`is_anomalous`, `is_optimised`).
- Emits JSON messages to Kafka with field names matching the `raw.*` Postgres columns.

Seeded RNG (default seed 42) makes runs reproducible.
