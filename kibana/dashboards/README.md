# Kibana dashboards

Exported saved objects (data views, visualisations, dashboards) for the dark-factory logs flowing through ELK.

## Files committed here

| File | Purpose |
|---|---|
| `00-bootstrap-data-view.ndjson` | Minimal data view (`darkfactory-logs-*`, `@timestamp` as time field). Import this **first** so the rest of Kibana has something to query against. |
| `dark_factory_overview.ndjson` | Full dashboard export. Created by you in the Kibana UI, then exported via *Stack Management → Saved Objects → Export*. Committed alongside the code so the portfolio reviewer sees a working dashboard. |

## First-time setup workflow

Run after `docker compose up -d` and confirming that Logstash is receiving logs (see "Verify logs are flowing" below).

1. Open Kibana at <http://localhost:5601>.
2. **Stack Management → Saved Objects → Import** → select `00-bootstrap-data-view.ndjson`.
3. Open **Discover**. You should immediately see log entries with fields `service`, `message`, `@timestamp`.
4. Build the dashboard (see "Recommended visualisations" below).
5. **Stack Management → Saved Objects** → tick the dashboard and its visualisations → **Export** → save as `dark_factory_overview.ndjson` in this directory.
6. Commit.

## Verify logs are flowing

```bash
# Confirm Logstash is listening on UDP 12201
docker compose logs logstash | grep "Successfully started Logstash"

# Confirm Elasticsearch has at least one index matching the pattern
curl -s "http://localhost:9200/_cat/indices/darkfactory-logs-*?v"
```

You should see one or more `darkfactory-logs-YYYY.MM.DD` indices with non-zero `docs.count`. If the count stays at zero, the GELF log driver isn't shipping — see `docs/setup.md` §8 (Troubleshooting).

## Recommended visualisations

Build these in **Visualize Library → Create new visualization** against the `darkfactory-logs-*` data view. They mirror what the portfolio reviewer expects to see.

| Visualisation | Type | Config |
|---|---|---|
| **Log volume over time** | Area chart | Y = `Count`. X = `@timestamp` (auto interval). Split series by `service.keyword`. |
| **Logs by service** | Pie or donut | Slice by `service.keyword`. |
| **Recent errors** | Data table | Filter: `message: ("ERROR" OR "Exception" OR "Traceback")`. Columns: `@timestamp`, `service`, `message`. Sort desc by time. Page size 50. |
| **DAG task heartbeat** | Bar chart | Filter: `service: "airflow-scheduler"`. Y = Count. X = `@timestamp` (1 min interval). Confirms the scheduler is alive. |

Combine all four onto a single dashboard named **Dark Factory Overview**. Save, then export per step 5 above.

## Why we don't pre-commit the full dashboard NDJSON

Kibana saved-object IDs are generated on import and the cross-references between dashboard → visualisation → data view need to match. Building the dashboard once in the UI and exporting the *complete* result is more robust than hand-writing the NDJSON. The conception doc's discipline note applies here: *"working code over clever code."*
