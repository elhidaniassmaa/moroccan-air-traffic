# ─────────────────────────────────────────────────────────────
# Moroccan Air Traffic Streaming Platform — Makefile
# ─────────────────────────────────────────────────────────────
.PHONY: help up down restart logs test lint dbt-build demo clean healthcheck

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Docker ───────────────────────────────────────────────────
up: ## Start all services (detached)
	docker compose up -d --build

down: ## Stop all services and remove volumes
	docker compose down -v

restart: down up ## Restart everything

logs: ## Tail all logs
	docker compose logs -f

# ── Demo mode (synthetic data, no API keys needed) ───────────
demo: ## Run full pipeline in synthetic/demo mode
	@echo "🚀 Starting demo mode (synthetic data)..."
	PRODUCER_SYNTHETIC_MODE=true docker compose up -d --build
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 15
	@echo "✅ Demo running. Redpanda Console: http://localhost:8080"
	@echo "   Dashboard: http://localhost:8501"

# ── Testing ──────────────────────────────────────────────────
test: ## Run all pytest tests
	python -m pytest tests/ -v --tb=short

test-unit: ## Run unit tests only
	python -m pytest tests/ -v --tb=short -m "not integration"

# ── Linting ──────────────────────────────────────────────────
lint: ## Run ruff linter + formatter check
	python -m ruff check .
	python -m ruff format --check .

lint-fix: ## Auto-fix lint issues
	python -m ruff check --fix .
	python -m ruff format .

lint-sql: ## Run sqlfluff on dbt models
	cd dbt_project && sqlfluff lint models/ --dialect duckdb

# ── dbt ──────────────────────────────────────────────────────
dbt-deps: ## Install dbt packages
	cd dbt_project && dbt deps

dbt-seed: ## Load seed data
	cd dbt_project && dbt seed

dbt-build: dbt-deps dbt-seed ## Full dbt build (deps + seed + build)
	cd dbt_project && dbt build

dbt-test: ## Run dbt tests only
	cd dbt_project && dbt test

dbt-docs: ## Generate and serve dbt docs
	cd dbt_project && dbt docs generate && dbt docs serve --port 8081

# ── Healthcheck ──────────────────────────────────────────────
healthcheck: ## Check all services are healthy
	python scripts/healthcheck.py

# ── Metrics ──────────────────────────────────────────────────
metrics: ## Measure and display pipeline metrics
	python scripts/measure_metrics.py

# ── Cleanup ──────────────────────────────────────────────────
clean: ## Remove local data lake and checkpoints
	rm -rf data/lake data/checkpoints
	rm -f data/analytics.duckdb
	@echo "🧹 Cleaned local data"

# ── Topics ───────────────────────────────────────────────────
create-topics: ## Create Redpanda topics
	docker compose exec redpanda rpk topic create flights.raw \
		--partitions 3 \
		--config cleanup.policy=delete \
		--config retention.ms=604800000
	docker compose exec redpanda rpk topic create flights.dlq \
		--partitions 1 \
		--config cleanup.policy=compact

list-topics: ## List Redpanda topics
	docker compose exec redpanda rpk topic list
