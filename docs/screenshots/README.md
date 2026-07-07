# Screenshots

Visual evidence of the end-to-end pipeline run. Each file corresponds to a step in `docs/verification.md`.

| File | Shows |
|---|---|
| `Docker_ps.png` | All eleven services healthy after `docker compose up -d` |
| `Data_Generator_1M.png` | Tail of `data_generator.bootstrap` output — 1 000 000 rows sent to Kafka |
| `Airflow_Pipeline_Graph.png` | The `dark_factory_pipeline` DAG graph view with all three tasks green |
| `Spark_Quarterly_Aggregation.png` | Spark master UI showing the aggregation application running |
| `Spark_Quarterly_Aggregation_Successful.png` | Spark master UI showing the aggregation application completed |
| `Spark_Analytics_Table.png` | `SELECT * FROM analytics.*` output confirming aggregation ran |
| `Logstash_6a.png` | Logstash startup logs, no unreachable-Elasticsearch warnings |
| `Elasticsearch_6b.png` | `_cat/indices/darkfactory-logs-*` output with non-zero `docs.count` |
| `Sanity_check_6c.png` | Sample log document via `_search`, showing `@timestamp` / `message` / `service` fields |
| `Elastic_Logs_6d.png` | Kibana Discover with log entries and the `service` field distributing across the instrumented services |
| `Log volume over time.png` | Kibana visualisation: log volume over time, split by `service` |
