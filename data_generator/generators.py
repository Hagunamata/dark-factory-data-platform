"""Row generation logic per source dataset.

TODO (Phase 2):
  - generate_logistics_event(timestamp, schema) -> dict
  - generate_hrss_reading(timestamp, schema) -> dict
  - Each function returns a single event/reading row as a dict ready to be
    JSON-serialised and sent to Kafka.
"""
