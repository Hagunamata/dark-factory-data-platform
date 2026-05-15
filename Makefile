# Dark Factory Data Platform — Makefile
# All commands assume Docker and Docker Compose v2 are installed.

.PHONY: help up down restart logs ps seed demo clean nuke status

help:  ## Show this help message
	@echo "Dark Factory Data Platform"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

up:  ## Bring the entire stack online
	docker compose up -d
	@echo ""
	@echo "Stack is starting. Services may take 30-60s to become healthy."
	@echo "Check status with: make status"

down:  ## Stop the stack but preserve volumes
	docker compose down

restart:  ## Restart all services
	docker compose restart

logs:  ## Tail logs from all services
	docker compose logs -f --tail=100

ps:  ## Show running services
	docker compose ps

status:  ## Show health status of all services
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

seed:  ## Generate synthetic data and load it
	docker compose exec data-generator python -m data_generator.bootstrap

demo:  ## Run a compressed end-to-end pipeline cycle (use after seed)
	docker compose exec airflow-webserver airflow dags trigger dark_factory_pipeline

clean:  ## Stop the stack and remove volumes (deletes all data!)
	docker compose down -v

nuke:  ## Full reset: stop, remove volumes, remove generated files
	docker compose down -v
	rm -rf data/ generated/ airflow/logs/
	@echo "All persistent state removed."
