# Kibana dashboards

Exported saved objects for the log dashboard.

| File | Purpose |
|---|---|
| `00-bootstrap-data-view.ndjson` | Minimal data view for `darkfactory-logs-*` with `@timestamp` as the time field. Import this first. |
| `dark_factory_overview.ndjson` | Optional full dashboard export, built and exported from the Kibana UI. |

## Setup

1. Open Kibana at http://localhost:5601.
2. **Stack Management → Saved Objects → Import** → select `00-bootstrap-data-view.ndjson`.
3. Open **Discover** and pick the *Dark Factory Logs* data view. Log entries with `@timestamp`, `service`, and `message` fields should appear.

## Recommended visualisations

Built against the `darkfactory-logs-*` data view:

- **Log volume over time** — area chart, Y = Count, X = `@timestamp`, split by `service.keyword`.
- **Logs by service** — pie, slice by `service.keyword`.
- **Recent errors** — data table filtered by `message: ("ERROR" OR "Exception" OR "Traceback")`, columns `@timestamp`, `service`, `message`.
- **Scheduler heartbeat** — bar chart filtered by `service: "airflow-scheduler"`, X = `@timestamp` (1 min bucket).

Combine them onto a dashboard named **Dark Factory Overview**, then export via *Stack Management → Saved Objects* as `dark_factory_overview.ndjson`.
