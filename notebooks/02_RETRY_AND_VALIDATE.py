# Databricks notebook source
# MAGIC %md
# MAGIC # Retry or validate a prior run
# MAGIC Reuses the original run inputs. Successful cached names are skipped, so
# MAGIC only failed or still-unprocessed names consume endpoint capacity.

# COMMAND ----------
from __future__ import annotations

import json
from pathlib import Path


def parameter(name: str, default: str = "") -> str:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    return dbutils.widgets.get(name).strip()


CATALOG = parameter("catalog")
SCHEMA = parameter("schema", "name_intelligence")
RUN_ID = parameter("run_id")
ENDPOINT = parameter("endpoint")
VOLUME = parameter("volume", "files")
BATCH_SIZE = parameter("batch_size", "20")
MAX_CONCURRENCY = parameter("max_concurrent_requests", "8")

if not all([CATALOG, SCHEMA, RUN_ID, ENDPOINT]):
    raise ValueError("catalog, schema, run_id, and endpoint are required")

row = spark.sql(f"""
SELECT input_path, selected_columns
FROM `{CATALOG}`.`{SCHEMA}`.`analysis_runs`
WHERE run_id = '{RUN_ID.replace("'", "''")}'
ORDER BY updated_at DESC LIMIT 1
""").first()
if row is None:
    raise ValueError(f"Run not found: {RUN_ID}")

context_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
batch_path = str(Path(context_path).parent / "01_BATCH_NAME_PIPELINE")

result = dbutils.notebook.run(
    batch_path,
    0,
    {
        "catalog": CATALOG,
        "schema": SCHEMA,
        "volume": VOLUME,
        "endpoint": ENDPOINT,
        "run_id": RUN_ID,
        "input_path": row.input_path,
        "selected_columns_json": json.dumps(row.selected_columns or []),
        "batch_size": BATCH_SIZE,
        "max_concurrent_requests": MAX_CONCURRENCY,
    },
)
print(result)
dbutils.notebook.exit(result)

