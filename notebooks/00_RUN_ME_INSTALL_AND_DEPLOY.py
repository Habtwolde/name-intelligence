# Databricks notebook source
# MAGIC %md
# MAGIC # Name Intelligence — one-run installer
# MAGIC Run this notebook from the imported project. It discovers the workspace,
# MAGIC creates the governed project assets, installs the batch job, grants the
# MAGIC app identity least-privilege access, deploys the app, and prints a report.

# COMMAND ----------
from __future__ import annotations

import json
import re
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient


def widget(name: str, default: str, label: str) -> str:
    try:
        dbutils.widgets.text(name, default, label)
    except Exception:
        pass
    return dbutils.widgets.get(name).strip()


PROJECT_NAME = widget("project_name", "name_intelligence", "Project name")
TARGET_CATALOG = widget("target_catalog", "AUTO", "Target catalog (AUTO = current)")
TARGET_SCHEMA = widget("target_schema", "name_intelligence", "Target schema")
VOLUME_NAME = widget("volume_name", "files", "Managed volume")
APP_NAME = widget("app_name", "name-intelligence-app", "Databricks App name")
WAREHOUSE_ID = widget("warehouse_id", "AUTO", "SQL warehouse ID")
REQUIRED_ENDPOINT_NAME = "databricks-meta-llama-3-3-70b-instruct"
CONFIGURED_ENDPOINT_NAME = widget("endpoint_name", REQUIRED_ENDPOINT_NAME, "Llama 3.3 70B model serving endpoint")
ENDPOINT_NAME = REQUIRED_ENDPOINT_NAME
AUTO_CREATE_WAREHOUSE = widget("auto_create_warehouse", "false", "Create a small serverless warehouse if none exists").lower() == "true"
WAREHOUSE_SIZE = widget("warehouse_size", "2X-Small", "New warehouse size")
BATCH_SIZE = int(widget("batch_size", "20", "Names per LLM request"))
CONFIGURED_MAX_CONCURRENCY = int(widget("max_concurrent_requests", "1", "Concurrent endpoint requests"))
# Llama pay-per-token endpoints reserve max_tokens for every in-flight request.
# Keep the portable installer at one request so legacy widget values cannot
# exceed a client's output-token-per-minute quota.
MAX_CONCURRENCY = 1
if CONFIGURED_MAX_CONCURRENCY != MAX_CONCURRENCY:
    print(
        f"Replacing legacy concurrency {CONFIGURED_MAX_CONCURRENCY} with "
        f"the Llama-safe value {MAX_CONCURRENCY}."
    )
MAX_NEW_NAMES = int(widget("max_new_names_per_run", "100000", "Maximum new names per run"))
RUN_TEST = widget("run_acceptance_test", "true", "Run bundled acceptance test").lower() == "true"


def valid_identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{label} must contain letters, numbers, and underscores and cannot start with a number")
    return value


valid_identifier(TARGET_SCHEMA, "target_schema")
valid_identifier(VOLUME_NAME, "volume_name")
if not 1 <= BATCH_SIZE <= 25:
    raise ValueError("batch_size must be between 1 and 25")
if not 1 <= MAX_CONCURRENCY <= 32:
    raise ValueError("max_concurrent_requests must be between 1 and 32")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Discover this workspace and project

# COMMAND ----------
w = WorkspaceClient()
workspace_id = w.get_workspace_id()
workspace_host = w.config.host
current_user = w.current_user.me().user_name

context_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
project_root = str(Path(context_path).parent.parent)
workspace_source_root = project_root if project_root.startswith("/Workspace/") else f"/Workspace{project_root}"
batch_notebook_path = f"{project_root}/notebooks/01_BATCH_NAME_PIPELINE"
test_notebook_path = f"{project_root}/notebooks/03_ACCEPTANCE_TESTS"

if TARGET_CATALOG.upper() == "AUTO":
    TARGET_CATALOG = spark.sql("SELECT current_catalog()").first()[0]
    if TARGET_CATALOG.lower() == "hive_metastore":
        available_catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
        preferred = [name for name in available_catalogs if name.lower() == "main"]
        alternatives = [
            name for name in available_catalogs
            if name.lower() not in {"hive_metastore", "system", "samples"}
        ]
        if preferred:
            TARGET_CATALOG = preferred[0]
        elif alternatives:
            TARGET_CATALOG = alternatives[0]
        else:
            raise RuntimeError("AUTO could not find a Unity Catalog catalog that supports managed volumes.")
valid_identifier(TARGET_CATALOG, "target_catalog")

print(json.dumps({
    "workspace_id": workspace_id,
    "workspace_host": workspace_host,
    "installer": current_user,
    "project_root": project_root,
    "catalog": TARGET_CATALOG,
    "schema": TARGET_SCHEMA,
}, indent=2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create the schema, volume, and Delta tables

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}` COMMENT 'Name Intelligence application assets'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`{VOLUME_NAME}` COMMENT 'Name Intelligence uploads and exports'")

ddl = [
f"""
CREATE TABLE IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`analysis_runs` (
  run_id STRING NOT NULL,
  input_path STRING,
  selected_columns ARRAY<STRING>,
  status STRING,
  source_rows BIGINT,
  unique_names BIGINT,
  new_names BIGINT,
  cached_names BIGINT,
  processed_names BIGINT,
  failed_names BIGINT,
  job_run_id BIGINT,
  started_at TIMESTAMP,
  updated_at TIMESTAMP,
  error_message STRING
) USING DELTA
""",
f"""
CREATE TABLE IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`source_name_values` (
  run_id STRING NOT NULL,
  row_id STRING NOT NULL,
  source_column STRING NOT NULL,
  original_name STRING,
  normalized_name STRING,
  name_hash STRING,
  ingested_at TIMESTAMP
) USING DELTA
""",
f"""
CREATE TABLE IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`name_registry` (
  name_hash STRING NOT NULL,
  normalized_name STRING NOT NULL,
  search_key STRING,
  frequency BIGINT,
  source_columns ARRAY<STRING>,
  first_seen TIMESTAMP,
  last_seen TIMESTAMP
) USING DELTA
""",
f"""
CREATE TABLE IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`name_analysis_cache` (
  name_hash STRING NOT NULL,
  normalized_name STRING NOT NULL,
  family_id STRING,
  response_json STRING,
  primary_name_tradition STRING,
  confidence DOUBLE,
  review_required BOOLEAN,
  status STRING,
  model_endpoint STRING,
  prompt_version STRING,
  processed_at TIMESTAMP,
  error_message STRING
) USING DELTA
""",
f"""
CREATE TABLE IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`name_families` (
  family_id STRING NOT NULL,
  canonical_name STRING,
  primary_name_tradition STRING,
  summary STRING,
  response_template_json STRING,
  model_endpoint STRING,
  prompt_version STRING,
  updated_at TIMESTAMP
) USING DELTA
""",
f"""
CREATE TABLE IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`name_family_members` (
  family_id STRING NOT NULL,
  member_name STRING NOT NULL,
  member_search_key STRING NOT NULL,
  relationship_type STRING,
  confidence DOUBLE,
  source_name_hash STRING,
  updated_at TIMESTAMP
) USING DELTA
""",
f"""
CREATE TABLE IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`name_relationships` (
  name_hash STRING NOT NULL,
  related_name STRING NOT NULL,
  related_search_key STRING,
  relationship_type STRING,
  cultural_context STRING,
  why STRING,
  confidence DOUBLE,
  model_endpoint STRING,
  prompt_version STRING,
  processed_at TIMESTAMP
) USING DELTA
""",
f"""
CREATE TABLE IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`name_review_feedback` (
  feedback_id STRING NOT NULL,
  name_hash STRING,
  submitted_by STRING,
  decision STRING,
  corrected_response_json STRING,
  notes STRING,
  submitted_at TIMESTAMP
) USING DELTA
""",
]
for statement in ddl:
    spark.sql(statement)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Select portable workspace resources

# COMMAND ----------
def state_value(value) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").upper()


warehouses = list(w.warehouses.list())
if WAREHOUSE_ID.upper() == "AUTO":
    ranked = sorted(
        warehouses,
        key=lambda item: (
            state_value(getattr(item, "state", "")) != "RUNNING",
            not bool(getattr(item, "enable_serverless_compute", False)),
            str(getattr(item, "name", "")),
        ),
    )
    if ranked:
        WAREHOUSE_ID = ranked[0].id
    elif AUTO_CREATE_WAREHOUSE:
        created = w.api_client.do("POST", "/api/2.0/sql/warehouses", body={
            "name": f"{PROJECT_NAME}-serverless",
            "cluster_size": WAREHOUSE_SIZE,
            "min_num_clusters": 1,
            "max_num_clusters": 1,
            "auto_stop_mins": 10,
            "warehouse_type": "PRO",
            "enable_serverless_compute": True,
        })
        WAREHOUSE_ID = created["id"]
    else:
        raise RuntimeError("No accessible SQL warehouse was found. Rerun with auto_create_warehouse=true or provide warehouse_id.")


def verify_chat_endpoint(name: str) -> bool:
    try:
        response = w.api_client.do(
            "POST",
            f"/serving-endpoints/{name}/invocations",
            body={
                "messages": [{"role": "user", "content": "Reply with OK."}],
                "temperature": 0,
                "max_tokens": 8,
            },
        )
        return bool(response.get("choices"))
    except Exception:
        return False


if CONFIGURED_ENDPOINT_NAME != REQUIRED_ENDPOINT_NAME:
    print(
        f"Replacing legacy endpoint selection {CONFIGURED_ENDPOINT_NAME!r} with "
        f"the required Llama 3.3 70B endpoint {REQUIRED_ENDPOINT_NAME!r}."
    )
if not verify_chat_endpoint(ENDPOINT_NAME):
    raise RuntimeError(
        f"Required endpoint '{ENDPOINT_NAME}' did not accept a chat-completions request. "
        "Confirm that Meta Llama 3.3 70B Instruct is available in this workspace region."
    )

print(f"Selected warehouse: {WAREHOUSE_ID}")
print(f"Selected endpoint: {ENDPOINT_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create or update the serverless batch job

# COMMAND ----------
job_name = f"{PROJECT_NAME}-batch-enrichment"
job_parameters = [
    {"name": "catalog", "default": TARGET_CATALOG},
    {"name": "schema", "default": TARGET_SCHEMA},
    {"name": "volume", "default": VOLUME_NAME},
    {"name": "endpoint", "default": ENDPOINT_NAME},
    {"name": "run_id", "default": ""},
    {"name": "input_path", "default": ""},
    {"name": "selected_columns_json", "default": "[]"},
    {"name": "batch_size", "default": str(BATCH_SIZE)},
    {"name": "max_concurrent_requests", "default": str(MAX_CONCURRENCY)},
    {"name": "max_new_names", "default": str(MAX_NEW_NAMES)},
]
job_settings = {
    "name": job_name,
    "max_concurrent_runs": 2,
    "parameters": job_parameters,
    "tasks": [{
        "task_key": "enrich_names",
        "notebook_task": {"notebook_path": batch_notebook_path, "source": "WORKSPACE"},
        "timeout_seconds": 0,
    }],
    "tags": {"project": PROJECT_NAME, "managed_by": "name-intelligence-installer"},
}
existing_jobs = [job for job in w.jobs.list(name=job_name) if job.settings and job.settings.name == job_name]
if existing_jobs:
    job_id = existing_jobs[0].job_id
    w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": job_settings})
else:
    job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=job_settings)["job_id"]
print(f"Batch job ID: {job_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create the app, grant its identity access, and deploy

# COMMAND ----------
app_resources = [
    {
        "name": "sql-warehouse",
        "description": "Warehouse used by the app",
        "sql_warehouse": {"id": WAREHOUSE_ID, "permission": "CAN_USE"},
    },
    {
        "name": "serving-endpoint",
        "description": "Llama endpoint used by the app",
        "serving_endpoint": {"name": ENDPOINT_NAME, "permission": "CAN_QUERY"},
    },
    {
        "name": "batch-job",
        "description": "Scalable name enrichment job",
        "job": {"id": str(job_id), "permission": "CAN_MANAGE_RUN"},
    },
    {
        "name": "project-volume",
        "description": "Uploads and exports volume",
        "uc_securable": {
            "securable_full_name": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{VOLUME_NAME}",
            "securable_type": "VOLUME",
            "permission": "WRITE_VOLUME",
        },
    },
]
app_description = "Scalable, culturally aware linguistic name analysis"

try:
    app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
except Exception:
    app = w.api_client.do("POST", "/api/2.0/apps", body={
        "name": APP_NAME,
        "description": app_description,
        "resources": app_resources,
    })
else:
    w.api_client.do(
        "POST",
        f"/api/2.0/apps/{APP_NAME}/update",
        body={"app": {
            "name": APP_NAME,
            "description": app_description,
            "resources": app_resources,
        }, "update_mask": "description,resources"},
    )
    for _ in range(120):
        update_status = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}/update")
        update_state = str(update_status.get("status", {}).get("state", "")).upper()
        if update_state in {"SUCCEEDED", "FAILED"}:
            break
        time.sleep(2)
    if update_state != "SUCCEEDED":
        raise RuntimeError(f"App resource update failed: {json.dumps(update_status, default=str)}")

for _ in range(60):
    app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
    service_principal = app.get("service_principal_client_id") or app.get("service_principal_id")
    if service_principal:
        break
    time.sleep(2)
else:
    raise RuntimeError("The app identity was not provisioned within two minutes.")


principal = service_principal.replace("`", "``")
grants = [
    f"GRANT USE CATALOG ON CATALOG `{TARGET_CATALOG}` TO `{principal}`",
    f"GRANT USE SCHEMA ON SCHEMA `{TARGET_CATALOG}`.`{TARGET_SCHEMA}` TO `{principal}`",
    f"GRANT SELECT, MODIFY ON ALL TABLES IN SCHEMA `{TARGET_CATALOG}`.`{TARGET_SCHEMA}` TO `{principal}`",
    f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`{VOLUME_NAME}` TO `{principal}`",
]
for statement in grants:
    spark.sql(statement)

deployment = w.api_client.do(
    "POST",
    f"/api/2.0/apps/{APP_NAME}/deployments",
    body={
        "source_code_path": workspace_source_root,
        "mode": "SNAPSHOT",
    },
)
deployment_id = deployment.get("deployment_id")

for _ in range(120):
    status = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}/deployments/{deployment_id}")
    raw_status = status.get("status")
    state = str(raw_status.get("state") if isinstance(raw_status, dict) else raw_status or "").upper()
    if any(token in state for token in ["SUCCEEDED", "FAILED", "CANCELLED"]):
        break
    time.sleep(5)

if "FAILED" in state or "CANCELLED" in state:
    raise RuntimeError(f"App deployment did not succeed: {json.dumps(status, default=str)}")

app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Optional bundled acceptance test

# COMMAND ----------
test_run_id = None
if RUN_TEST:
    try:
        test_result = dbutils.notebook.run(
            test_notebook_path,
            900,
            {
                "catalog": TARGET_CATALOG,
                "schema": TARGET_SCHEMA,
                "volume": VOLUME_NAME,
                "endpoint": ENDPOINT_NAME,
                "job_id": str(job_id),
                "project_root": project_root,
            },
        )
        test_run_id = test_result
    except Exception as exc:
        test_run_id = f"Acceptance test warning: {exc}"

# COMMAND ----------
report = {
    "installation": "SUCCEEDED",
    "workspace_id": workspace_id,
    "workspace_host": workspace_host,
    "catalog": TARGET_CATALOG,
    "schema": TARGET_SCHEMA,
    "volume": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{VOLUME_NAME}",
    "warehouse_id": WAREHOUSE_ID,
    "endpoint": ENDPOINT_NAME,
    "batch_job_id": job_id,
    "app_name": APP_NAME,
    "app_url": app.get("url"),
    "deployment_id": deployment_id,
    "service_principal": service_principal,
    "acceptance_test": test_run_id,
}
print(json.dumps(report, indent=2, default=str))
dbutils.notebook.exit(json.dumps(report, default=str))
