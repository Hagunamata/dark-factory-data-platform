# Portfolio screenshots — capture checklist

This directory holds the visual evidence the course assessor will look at. Capture these after `docs/verification.md` passes end-to-end. PNG, full-window, no cropping unless noted.

## Required (referenced from the portfolio brief)

| # | Filename | What to capture | Where |
|---|---|---|---|
| 1 | `01-airflow-dag-graph.png` | The `dark_factory_pipeline` DAG **Graph view** after a successful run. All three task boxes dark green. | Airflow UI → DAGs → dark_factory_pipeline → Graph |
| 2 | `02-airflow-dag-runs.png` | The DAG **Runs** tab showing at least one successful run with task durations. | Airflow UI → DAGs → dark_factory_pipeline → Grid (with a successful run selected) |
| 3 | `03-spark-master-ui.png` | The Spark master UI **after the aggregation job has completed**. Worker registered, completed application visible. | http://localhost:8081 |
| 4 | `04-spark-job-detail.png` | Drill into the completed application → stages tab, showing actual stage timings. | http://localhost:8081 → Application ID → Stages |
| 5 | `05-postgres-raw-count.png` | A terminal screenshot of a SQL query against `raw.*` showing row counts. Suggested query:<br>`SELECT 'logistics', COUNT(*) FROM raw.logistics_events UNION ALL SELECT 'hrss', COUNT(*) FROM raw.hrss_telemetry;` | Local terminal |
| 6 | `06-postgres-analytics-sample.png` | A `SELECT * FROM analytics.logistics_features_quarterly LIMIT 10;` showing real values. Crop to the data area if the query line is far above. | Local terminal |
| 7 | `07-kibana-discover.png` | Kibana **Discover** view with the `darkfactory-logs-*` data view selected, showing live log entries. Hover the `service` field to show its value distribution. | http://localhost:5601 → Discover |
| 8 | `08-kibana-dashboard.png` | The full **Dark Factory Overview** dashboard you built per `kibana/dashboards/README.md`. | http://localhost:5601 → Dashboard → Dark Factory Overview |

## Optional (strengthens the portfolio)

| # | Filename | What to capture |
|---|---|---|
| 9 | `09-docker-ps.png` | Output of `docker compose ps` with every service in a healthy state. Demonstrates the IaC claim. |
| 10 | `10-data-generator-run.png` | The tail of the `make seed` output showing the final `Bootstrap complete: 1000000 rows total` line and the throughput numbers. |
| 11 | `11-elasticsearch-indices.png` | Output of `curl http://localhost:9200/_cat/indices/darkfactory-logs-*?v` showing doc counts. |
| 12 | `12-airflow-task-log.png` | A successful `spark_quarterly_aggregation` task log in the Airflow UI, showing the `Wrote N rows to analytics.* for quarter ...` lines. |

## Tips

- Use the OS-level screenshot tool (Snipping Tool on Windows, `gnome-screenshot` on Ubuntu). Browser screenshots tend to crop URL bars in ways that obscure context.
- Resize the browser window so the relevant content fills the frame — assessor screen real estate is finite.
- For SQL screenshots, run the query first, *then* take the shot. Captures that include "press any key to continue" prompts look unfinished.
- Crop terminal screenshots to the relevant region. Don't include a forest of preceding commands.
- Name files exactly as listed so they sort in capture order in `02-development.md` and any portfolio gallery.

## After capture

1. Commit them to `docs/screenshots/`.
2. Reference them in `docs/02-development.md` §5.1 (the "Observations from the local run" checklist becomes a paragraph + image references).
3. Tag the release: `git tag v0.2-phase2-complete && git push --tags`.
