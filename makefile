DAGSTER_DIR := dagster_project
API_DIR     := api

.PHONY: dagster api

dagster:
	cd $(DAGSTER_DIR) && dotenvx run --env-file .env.local -- \
		.venv/bin/dagster dev

api:
	cd $(API_DIR) && dotenvx run --env-file .env -- \
		uv run fastapi dev main.py
