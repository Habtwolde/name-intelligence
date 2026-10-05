# Databricks notebook source
# MAGIC %md
# MAGIC # Acceptance tests
# MAGIC Uploads the bundled generic-header CSV, launches the real batch job, and
# MAGIC verifies automatic column selection, caching, result limits, and output.

# COMMAND ----------
from __future__ import annotations

import json
from pathlib import Path
import time
import uuid

from databricks.sdk import WorkspaceClient


def parameter(name: str, default: str = "") -> str:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    return dbutils.widgets.get(name).strip()


CATALOG = parameter("catalog")
SCHEMA = parameter("schema", "name_intelligence")
VOLUME = parameter("volume", "files")
ENDPOINT = parameter("endpoint")
JOB_ID = int(parameter("job_id", "0"))
PROJECT_ROOT = parameter("project_root")

if not all([CATALOG, SCHEMA, VOLUME, ENDPOINT, JOB_ID, PROJECT_ROOT]):
    raise ValueError("Acceptance-test parameters are incomplete")

w = WorkspaceClient()
local_root = PROJECT_ROOT if PROJECT_ROOT.startswith("/Workspace/") else f"/Workspace{PROJECT_ROOT}"
sample_local = f"{local_root}/sample_names_test.csv"
run_id = f"acceptance-{uuid.uuid4()}"
sample_volume = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/acceptance/{run_id}/sample_names_test.csv"

with open(sample_local, "rb") as handle:
    w.files.upload(sample_volume, handle, overwrite=True)

run = w.jobs.run_now(
    job_id=JOB_ID,
    job_parameters={
        "catalog": CATALOG,
        "schema": SCHEMA,
        "volume": VOLUME,
        "endpoint": ENDPOINT,
        "run_id": run_id,
        "input_path": sample_volume,
        "selected_columns_json": "[]",
        "batch_size": "20",
        "max_concurrent_requests": "2",
        "max_new_names": "100",
    },
)

deadline = time.time() + 780
while time.time() < deadline:
    status = w.jobs.get_run(run.run_id)
    life_cycle = str(getattr(status.state.life_cycle_state, "value", status.state.life_cycle_state))
    if life_cycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
        result_state = str(getattr(status.state.result_state, "value", status.state.result_state))
        break
    time.sleep(10)
else:
    raise TimeoutError(f"Acceptance run {run.run_id} did not finish within 13 minutes")

if result_state not in {"SUCCESS", "SUCCEEDED"}:
    raise AssertionError(f"Acceptance job failed: lifecycle={life_cycle}, result={result_state}")

run_row = spark.sql(f"""
SELECT * FROM `{CATALOG}`.`{SCHEMA}`.`analysis_runs` WHERE run_id='{run_id}' ORDER BY updated_at DESC LIMIT 1
""").first()
assert run_row is not None, "analysis_runs did not receive the test run"
assert run_row.source_rows == 30, f"expected 30 source rows, found {run_row.source_rows}"
assert set(run_row.selected_columns or []) == {"FIELD_A", "FIELD_B"}, f"unexpected detected columns: {run_row.selected_columns}"
assert run_row.unique_names > 20, "too few unique names were extracted"
assert run_row.status in {"COMPLETED", "COMPLETED_WITH_ERRORS"}, f"unexpected status: {run_row.status}"

violations = spark.sql(f"""
SELECT name_hash, relationship_type, count(*) AS n
FROM `{CATALOG}`.`{SCHEMA}`.`name_relationships`
GROUP BY name_hash, relationship_type
HAVING count(*) > 5
""").count()
assert violations == 0, "a result contains more than five relationships of one type"

export_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/exports/{run_id}"
assert dbutils.fs.ls(export_path), "enriched export was not created"

result = {
    "status": "PASSED",
    "run_id": run_id,
    "job_run_id": run.run_id,
    "detected_columns": run_row.selected_columns,
    "source_rows": run_row.source_rows,
    "unique_names": run_row.unique_names,
    "processed_names": run_row.processed_names,
    "failed_names": run_row.failed_names,
    "export_path": export_path,
}
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))

