"""One-shot bulk generator for the initial historical data load.

Generates the full configured row count for both sources, distributes the
timestamps across the 18-month time window, and pushes everything to Kafka.

Intended to be run once after `make up`, via `make seed`.

TODO (Phase 2):
  - Read configuration from environment variables (see .env.example)
  - Load source schemas via data_generator.schemas
  - Generate rows in batches and push to the configured Kafka topics
  - Log progress to stdout so make seed gives useful feedback
"""


def main():
    raise NotImplementedError(
        "bootstrap.py is a Phase 2 implementation task. See module docstring."
    )


if __name__ == "__main__":
    main()
