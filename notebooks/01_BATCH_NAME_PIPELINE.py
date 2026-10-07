# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental name-enrichment pipeline
# MAGIC This notebook is normally launched by the app. It processes only unseen
# MAGIC normalized names, reuses family matches, batches endpoint calls, and can
# MAGIC safely resume after interruption.

# COMMAND ----------
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
import traceback

import pandas as pd
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window


def get_parameter(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    return dbutils.widgets.get(name).strip()


CATALOG = get_parameter("catalog", "")
SCHEMA = get_parameter("schema", "name_intelligence")
VOLUME = get_parameter("volume", "files")
ENDPOINT = get_parameter("endpoint", "")
RUN_ID = get_parameter("run_id", "")
INPUT_PATH = get_parameter("input_path", "")
SELECTED_COLUMNS = json.loads(get_parameter("selected_columns_json", "[]"))
BATCH_SIZE = int(get_parameter("batch_size", "20"))
MAX_CONCURRENCY = int(get_parameter("max_concurrent_requests", "8"))
MAX_NEW_NAMES = int(get_parameter("max_new_names", "100000"))

if not all([CATALOG, SCHEMA, VOLUME, ENDPOINT, RUN_ID, INPUT_PATH]):
    raise ValueError("catalog, schema, volume, endpoint, run_id, and input_path are required")

context_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
project_root = str(Path(context_path).parent.parent)
local_root = project_root if project_root.startswith("/Workspace/") else f"/Workspace{project_root}"
sys.path.insert(0, f"{local_root}/src")

from name_intelligence.batching import chunked
from name_intelligence.detection import detect_name_columns, selected_columns
from name_intelligence.normalization import normalize_name, search_key, stable_name_hash
from name_intelligence.prompting import PROMPT_VERSION, SYSTEM_PROMPT, build_user_prompt, response_schema
from name_intelligence.validation import validate_analysis


TABLE = lambda name: f"`{CATALOG}`.`{SCHEMA}`.`{name}`"
w = WorkspaceClient()

# COMMAND ----------
def update_run(**values) -> None:
    assignments = []
    params = []
    for key, value in values.items():
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            assignments.append(f"{key} = '{escaped}'")
        elif value is None:
            assignments.append(f"{key} = NULL")
        else:
            assignments.append(f"{key} = {value}")
    assignments.append("updated_at = current_timestamp()")
    spark.sql(f"UPDATE {TABLE('analysis_runs')} SET {', '.join(assignments)} WHERE run_id = '{RUN_ID}'")


selected_sql = ", ".join("'" + str(column).replace("'", "''") + "'" for column in SELECTED_COLUMNS)
selected_array_sql = f"array({selected_sql})" if SELECTED_COLUMNS else "CAST(array() AS ARRAY<STRING>)"
spark.sql(f"""
MERGE INTO {TABLE('analysis_runs')} t
USING (SELECT '{RUN_ID}' run_id) s
ON t.run_id = s.run_id
WHEN MATCHED THEN UPDATE SET
  input_path = '{INPUT_PATH.replace("'", "''")}',
  selected_columns = {selected_array_sql},
  status = 'RUNNING', started_at = current_timestamp(), updated_at = current_timestamp(), error_message = NULL
WHEN NOT MATCHED THEN INSERT (
  run_id, input_path, selected_columns, status, source_rows, unique_names, new_names,
  cached_names, processed_names, failed_names, started_at, updated_at
) VALUES (
  '{RUN_ID}', '{INPUT_PATH.replace("'", "''")}', {selected_array_sql}, 'RUNNING', 0, 0, 0, 0, 0, 0,
  current_timestamp(), current_timestamp()
)
""")

# COMMAND ----------
try:
    source = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", False)
        .option("escape", '"')
        .csv(INPUT_PATH)
    )
    source_rows = source.count()

    if not SELECTED_COLUMNS:
        sample_pdf = source.limit(5000).toPandas()
        SELECTED_COLUMNS = selected_columns(detect_name_columns(sample_pdf))
    missing = [column for column in SELECTED_COLUMNS if column not in source.columns]
    if missing:
        raise ValueError(f"Selected columns do not exist in the CSV: {missing}")
    if not SELECTED_COLUMNS:
        raise ValueError("No name-like columns were detected")

    detected_sql = ", ".join("'" + str(column).replace("'", "''") + "'" for column in SELECTED_COLUMNS)
    spark.sql(f"""
    UPDATE {TABLE('analysis_runs')}
    SET selected_columns=array({detected_sql}), updated_at=current_timestamp()
    WHERE run_id='{RUN_ID}'
    """)

    @F.pandas_udf(T.StringType())
    def normalize_udf(values: pd.Series) -> pd.Series:
        return values.map(normalize_name)

    @F.pandas_udf(T.StringType())
    def search_key_udf(values: pd.Series) -> pd.Series:
        return values.map(search_key)

    pairs = F.array(*[
        F.struct(F.lit(column).alias("source_column"), F.col(column).cast("string").alias("original_name"))
        for column in SELECTED_COLUMNS
    ])
    long_names = (
        source.withColumn("_row_number", F.monotonically_increasing_id())
        .withColumn("row_id", F.sha2(F.concat_ws("|", F.lit(RUN_ID), F.col("_row_number")), 256))
        .select("row_id", F.explode(pairs).alias("pair"))
        .select("row_id", "pair.source_column", "pair.original_name")
        .withColumn("normalized_name", normalize_udf("original_name"))
        .filter((F.length("normalized_name") > 0) & (F.length("normalized_name") <= 120))
        .withColumn("name_hash", F.sha2("normalized_name", 256))
        .withColumn("run_id", F.lit(RUN_ID))
        .withColumn("ingested_at", F.current_timestamp())
    )

    spark.sql(f"DELETE FROM {TABLE('source_name_values')} WHERE run_id = '{RUN_ID}'")
    long_names.select(
        "run_id", "row_id", "source_column", "original_name", "normalized_name", "name_hash", "ingested_at"
    ).write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.source_name_values")

    impacted_hashes = long_names.select("name_hash").distinct()
    aggregate = (
        spark.table(f"{CATALOG}.{SCHEMA}.source_name_values")
        .join(impacted_hashes, "name_hash", "inner")
        .groupBy("name_hash", "normalized_name")
        .agg(
            F.count("*").alias("frequency"),
            F.array_sort(F.collect_set("source_column")).alias("source_columns"),
            F.min("ingested_at").alias("first_seen"),
            F.max("ingested_at").alias("last_seen"),
        )
        .withColumn("search_key", search_key_udf("normalized_name"))
    )
    aggregate.createOrReplaceTempView("ni_registry_updates")
    spark.sql(f"""
    MERGE INTO {TABLE('name_registry')} t
    USING ni_registry_updates s ON t.name_hash = s.name_hash
    WHEN MATCHED THEN UPDATE SET
      normalized_name=s.normalized_name, search_key=s.search_key, frequency=s.frequency,
      source_columns=s.source_columns, first_seen=least(t.first_seen,s.first_seen), last_seen=s.last_seen
    WHEN NOT MATCHED THEN INSERT *
    """)

    unique_names = long_names.select("name_hash").distinct().count()
    update_run(source_rows=source_rows, unique_names=unique_names)

    # Reuse a previously validated family whenever the new spelling already
    # appears as a high-confidence family member.
    def successful_cache_hashes():
        return (
            spark.table(f"{CATALOG}.{SCHEMA}.name_analysis_cache")
            .filter(
                (F.col("status") == "SUCCESS")
                & (F.col("model_endpoint") == ENDPOINT)
                & (F.col("prompt_version") == PROMPT_VERSION)
            )
            .select("name_hash")
        )

    uncached = (
        aggregate.alias("r")
        .join(successful_cache_hashes(), "name_hash", "left_anti")
    )
    family_candidates = (
        uncached.alias("r")
        .join(
            spark.table(f"{CATALOG}.{SCHEMA}.name_family_members").filter("confidence >= 0.85").alias("m"),
            F.col("r.search_key") == F.col("m.member_search_key"),
            "inner",
        )
        .join(
            spark.table(f"{CATALOG}.{SCHEMA}.name_families")
            .filter(
                (F.col("model_endpoint") == ENDPOINT)
                & (F.col("prompt_version") == PROMPT_VERSION)
            )
            .alias("f"),
            "family_id",
            "inner",
        )
        .select(
            F.col("r.name_hash"), F.col("r.normalized_name"), "family_id",
            F.col("f.response_template_json").alias("response_json"),
            F.col("f.primary_name_tradition"), F.col("m.confidence"),
            F.lit(False).alias("review_required"), F.lit("SUCCESS").alias("status"),
            F.col("f.model_endpoint"), F.col("f.prompt_version"),
            F.current_timestamp().alias("processed_at"), F.lit(None).cast("string").alias("error_message"),
            F.row_number().over(Window.partitionBy("r.name_hash").orderBy(F.col("m.confidence").desc())).alias("rn"),
        )
        .filter("rn = 1").drop("rn")
    )
    if family_candidates.take(1):
        family_candidates.createOrReplaceTempView("ni_family_reuse")
        spark.sql(f"""
        MERGE INTO {TABLE('name_analysis_cache')} t USING ni_family_reuse s ON t.name_hash=s.name_hash
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        """)

    cached_names = (
        aggregate.select("name_hash").join(successful_cache_hashes(), "name_hash", "inner").count()
    )
    update_run(cached_names=cached_names)

    unresolved = (
        aggregate.join(successful_cache_hashes(), "name_hash", "left_anti")
        .orderBy(F.desc("frequency"), F.asc("normalized_name"))
        .limit(MAX_NEW_NAMES)
    )
    names_to_process = [row.asDict() for row in unresolved.select("name_hash", "normalized_name", "search_key").toLocalIterator()]
    update_run(new_names=len(names_to_process))

except Exception as exc:
    update_run(status="FAILED", error_message=f"Ingestion failed: {exc}")
    raise

# COMMAND ----------
def extract_content(response: dict) -> str:
    content = response["choices"][0]["message"]["content"]
    if isinstance(content, list):
        content = "".join(part.get("text", "") if isinstance(part, dict) else str(part) for part in content)
    text = str(content).strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0]
    return text.strip()


def call_endpoint(batch: list[dict]) -> list[dict]:
    names = [item["normalized_name"] for item in batch]
    payload = {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT + " Return valid JSON only."},
            {"role": "user", "content": build_user_prompt(names)},
        ],
        "temperature": 0.1,
        "max_tokens": max(1600, 750 * len(names)),
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "name_analysis", "strict": True, "schema": response_schema()},
        },
    }
    last_error = None
    for attempt in range(4):
        try:
            response = w.api_client.do("POST", f"/serving-endpoints/{ENDPOINT}/invocations", body=payload)
            parsed = json.loads(extract_content(response))
            items = parsed.get("items", parsed if isinstance(parsed, list) else [])
            output = []
            for index, source_item in enumerate(batch):
                raw = items[index] if index < len(items) and isinstance(items[index], dict) else {}
                cleaned, issues = validate_analysis(raw, source_item["normalized_name"])
                output.append({"source": source_item, "analysis": cleaned, "issues": issues, "error": None})
            return output
        except Exception as exc:
            last_error = exc
            if attempt == 0:
                payload.pop("response_format", None)
            time.sleep(min(30, 2 ** attempt))
    return [{"source": item, "analysis": None, "issues": [], "error": str(last_error)} for item in batch]


def family_id_for(name: str, analysis: dict) -> str:
    members = [search_key(name)]
    members.extend(
        search_key(rel["name"])
        for rel in analysis.get("relationships", [])
        if rel.get("relationship_type") in {"orthographic_variant", "transliteration_variant", "phonetic_variant"}
        and float(rel.get("confidence", 0)) >= 0.80
    )
    material = analysis.get("primary_name_tradition", "unknown").casefold() + "|" + "|".join(sorted(set(members)))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


cache_schema = T.StructType([
    T.StructField("name_hash", T.StringType(), False), T.StructField("normalized_name", T.StringType(), False),
    T.StructField("family_id", T.StringType()), T.StructField("response_json", T.StringType()),
    T.StructField("primary_name_tradition", T.StringType()), T.StructField("confidence", T.DoubleType()),
    T.StructField("review_required", T.BooleanType()), T.StructField("status", T.StringType()),
    T.StructField("model_endpoint", T.StringType()), T.StructField("prompt_version", T.StringType()),
    T.StructField("processed_at", T.TimestampType()), T.StructField("error_message", T.StringType()),
])


def flush_results(results: list[dict]) -> tuple[int, int]:
    if not results:
        return 0, 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cache_rows, family_rows, member_rows, relationship_rows = [], [], [], []
    success = failure = 0
    for result in results:
        source_item = result["source"]
        if result["error"]:
            failure += 1
            cache_rows.append((
                source_item["name_hash"], source_item["normalized_name"], None, None, None, 0.0, True,
                "FAILED", ENDPOINT, PROMPT_VERSION, now, result["error"][:4000],
            ))
            continue
        success += 1
        analysis = result["analysis"]
        family_id = family_id_for(source_item["normalized_name"], analysis)
        response_json = json.dumps(analysis, ensure_ascii=False, separators=(",", ":"))
        cache_rows.append((
            source_item["name_hash"], source_item["normalized_name"], family_id, response_json,
            analysis.get("primary_name_tradition", "Unknown"), float(analysis.get("confidence", 0)),
            bool(analysis.get("review_required", False)), "SUCCESS", ENDPOINT, PROMPT_VERSION, now, None,
        ))
        family_rows.append((
            family_id, source_item["normalized_name"], analysis.get("primary_name_tradition", "Unknown"),
            analysis.get("summary", ""), response_json, ENDPOINT, PROMPT_VERSION, now,
        ))
        member_rows.append((family_id, source_item["normalized_name"], source_item["search_key"], "self", 1.0, source_item["name_hash"], now))
        for rel in analysis.get("relationships", []):
            relationship_rows.append((
                source_item["name_hash"], rel["name"], search_key(rel["name"]), rel["relationship_type"],
                rel["cultural_context"], rel["why"], float(rel["confidence"]), ENDPOINT, PROMPT_VERSION, now,
            ))
            if rel["relationship_type"] in {"orthographic_variant", "transliteration_variant", "phonetic_variant"} and float(rel["confidence"]) >= 0.80:
                member_rows.append((family_id, rel["name"], search_key(rel["name"]), rel["relationship_type"], float(rel["confidence"]), source_item["name_hash"], now))

    cache_df = spark.createDataFrame(cache_rows, cache_schema)
    cache_df.createOrReplaceTempView("ni_cache_batch")
    spark.sql(f"""
    MERGE INTO {TABLE('name_analysis_cache')} t USING ni_cache_batch s ON t.name_hash=s.name_hash
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
    """)

    if family_rows:
        family_df = spark.createDataFrame(family_rows, "family_id string, canonical_name string, primary_name_tradition string, summary string, response_template_json string, model_endpoint string, prompt_version string, updated_at timestamp")
        family_df.createOrReplaceTempView("ni_family_batch")
        spark.sql(f"""MERGE INTO {TABLE('name_families')} t USING ni_family_batch s ON t.family_id=s.family_id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    if member_rows:
        member_df = spark.createDataFrame(member_rows, "family_id string, member_name string, member_search_key string, relationship_type string, confidence double, source_name_hash string, updated_at timestamp")
        member_df.createOrReplaceTempView("ni_member_batch")
        spark.sql(f"""MERGE INTO {TABLE('name_family_members')} t USING ni_member_batch s
        ON t.family_id=s.family_id AND t.member_search_key=s.member_search_key
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    if relationship_rows:
        rel_df = spark.createDataFrame(relationship_rows, "name_hash string, related_name string, related_search_key string, relationship_type string, cultural_context string, why string, confidence double, model_endpoint string, prompt_version string, processed_at timestamp")
        rel_df.createOrReplaceTempView("ni_relationship_batch")
        spark.sql(f"""MERGE INTO {TABLE('name_relationships')} t USING ni_relationship_batch s
        ON t.name_hash=s.name_hash AND t.relationship_type=s.relationship_type AND t.related_search_key=s.related_search_key
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    return success, failure

# COMMAND ----------
processed = failed = 0
pending_results: list[dict] = []
try:
    batches = list(chunked(names_to_process, BATCH_SIZE))
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
        futures = [executor.submit(call_endpoint, batch) for batch in batches]
        for future in as_completed(futures):
            pending_results.extend(future.result())
            if len(pending_results) >= max(100, BATCH_SIZE * MAX_CONCURRENCY * 2):
                ok, bad = flush_results(pending_results)
                processed += ok
                failed += bad
                pending_results = []
                update_run(processed_names=processed, failed_names=failed)
    ok, bad = flush_results(pending_results)
    processed += ok
    failed += bad

    export_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/exports/{RUN_ID}"
    enriched = (
        spark.table(f"{CATALOG}.{SCHEMA}.source_name_values").filter(F.col("run_id") == RUN_ID).alias("s")
        .join(spark.table(f"{CATALOG}.{SCHEMA}.name_analysis_cache").alias("a"), "name_hash", "left")
        .select(
            F.col("s.row_id"), F.col("s.source_column"), F.col("s.original_name"), F.col("s.normalized_name"),
            F.col("a.primary_name_tradition"), F.col("a.confidence"), F.col("a.review_required"),
            F.col("a.response_json"), F.col("a.status").alias("analysis_status"),
        )
    )
    enriched.write.mode("overwrite").option("header", True).csv(export_path)
    final_status = "COMPLETED_WITH_ERRORS" if failed else "COMPLETED"
    update_run(status=final_status, processed_names=processed, failed_names=failed)
    dbutils.notebook.exit(json.dumps({
        "run_id": RUN_ID, "status": final_status, "processed": processed, "failed": failed,
        "export_path": export_path,
    }))
except Exception as exc:
    update_run(status="FAILED", processed_names=processed, failed_names=failed, error_message=str(exc)[:4000])
    traceback.print_exc()
    raise
