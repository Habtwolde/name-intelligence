from __future__ import annotations

import io
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from name_intelligence.detection import detect_name_columns, selected_columns
from name_intelligence.normalization import normalize_name, stable_name_hash
from name_intelligence.prompting import (
    PROMPT_VERSION,
    SINGLE_PROMPT_VERSION,
    SINGLE_NAME_SYSTEM_PROMPT,
    SYSTEM_PROMPT,
    build_single_name_prompt,
    build_user_prompt,
    response_schema,
    single_name_response_schema,
)
from name_intelligence.validation import validate_analysis


st.set_page_config(page_title="Name Intelligence", page_icon="◈", layout="wide")


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def volume_coordinates(path: str) -> tuple[str, str, str]:
    """Return catalog, schema, and volume from a Databricks UC volume path."""
    parts = [part for part in path.strip().split("/") if part]
    if len(parts) >= 4 and parts[0].casefold() == "volumes":
        return parts[1], parts[2], parts[3]
    return "", "", ""


RESOURCE_CATALOG, RESOURCE_SCHEMA, RESOURCE_VOLUME = volume_coordinates(env("NI_VOLUME_PATH"))
CATALOG = env("NI_CATALOG", RESOURCE_CATALOG)
SCHEMA = env("NI_SCHEMA", RESOURCE_SCHEMA or "name_intelligence")
VOLUME = env("NI_VOLUME", RESOURCE_VOLUME or "files")
WAREHOUSE_ID = env("NI_WAREHOUSE_ID")
ENDPOINT = env("NI_ENDPOINT")
JOB_ID = env("NI_JOB_ID")
BATCH_SIZE = int(env("NI_BATCH_SIZE", "20"))
ACTIVE_PROMPT_VERSION = env("NI_PROMPT_VERSION", PROMPT_VERSION)


@st.cache_resource
def workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


@st.cache_resource
def sdk_config() -> Config:
    return Config()


def sql_connection():
    cfg = sdk_config()

    def credentials_provider():
        return cfg.authenticate

    return sql.connect(
        server_hostname=cfg.host.removeprefix("https://"),
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=credentials_provider,
        _use_arrow_native_complex_types=False,
    )


def query_frame(statement: str, parameters: list | None = None) -> pd.DataFrame:
    with sql_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement, parameters=parameters or [])
            rows = cursor.fetchall()
            columns = [item[0] for item in cursor.description] if cursor.description else []
    return pd.DataFrame(rows, columns=columns)


def execute(statement: str, parameters: list | None = None) -> None:
    with sql_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement, parameters=parameters or [])


def model_query(names: list[str], context: str = "", rich_single: bool = False) -> list[dict]:
    if rich_single and len(names) != 1:
        raise ValueError("Rich single-name analysis accepts exactly one name")
    payload = {
        "messages": [
            {"role": "system", "content": SINGLE_NAME_SYSTEM_PROMPT if rich_single else SYSTEM_PROMPT},
            {"role": "user", "content": build_single_name_prompt(names[0], context) if rich_single else build_user_prompt(names, context)},
        ],
        "temperature": 0.1,
        "max_tokens": 4000 if rich_single else max(1200, 750 * len(names)),
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "rich_name_analysis" if rich_single else "name_analysis",
                "strict": True,
                "schema": single_name_response_schema() if rich_single else response_schema(),
            },
        },
    }
    try:
        response = workspace_client().api_client.do(
            "POST", f"/serving-endpoints/{ENDPOINT}/invocations", body=payload
        )
    except Exception:
        payload.pop("response_format", None)
        response = workspace_client().api_client.do(
            "POST", f"/serving-endpoints/{ENDPOINT}/invocations", body=payload
        )
    content = response["choices"][0]["message"]["content"]
    if isinstance(content, list):
        content = "".join(part.get("text", "") if isinstance(part, dict) else str(part) for part in content)
    text = str(content).strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0]
    parsed = json.loads(text)
    items = parsed.get("items", parsed if isinstance(parsed, list) else [])
    output = []
    for index, name in enumerate(names):
        raw = items[index] if index < len(items) else {}
        cleaned, _ = validate_analysis(raw, name)
        output.append(cleaned)
    return output


def single_cache_key(normalized: str, context: str) -> str:
    value = "\0".join(
        [normalized, context.strip().casefold(), ENDPOINT, SINGLE_PROMPT_VERSION]
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def cache_single_result(normalized: str, context: str, result: dict) -> None:
    response_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    execute(
        f"""
        MERGE INTO `{CATALOG}`.`{SCHEMA}`.`single_name_analysis_cache` t
        USING (SELECT ? AS cache_key, ? AS name_hash, ? AS normalized_name) s
        ON t.cache_key=s.cache_key
        WHEN MATCHED THEN UPDATE SET
          normalized_name=s.normalized_name, requested_context=?, response_json=?,
          model_endpoint=?, prompt_version=?, processed_at=current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
          cache_key, name_hash, normalized_name, requested_context, response_json,
          model_endpoint, prompt_version, processed_at
        ) VALUES (s.cache_key, s.name_hash, s.normalized_name, ?, ?, ?, ?, current_timestamp())
        """,
        [
            single_cache_key(normalized, context), stable_name_hash(normalized), normalized,
            context.strip(), response_json, ENDPOINT, SINGLE_PROMPT_VERSION,
            context.strip(), response_json, ENDPOINT, SINGLE_PROMPT_VERSION,
        ],
    )


def upload_to_volume(uploaded_file, run_id: str) -> str:
    safe_name = Path(uploaded_file.name).name.replace(" ", "_")
    volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/uploads/{run_id}/{safe_name}"
    uploaded_file.seek(0)
    workspace_client().files.upload(volume_path, uploaded_file, overwrite=True)
    uploaded_file.seek(0)
    return volume_path


def start_batch_job(run_id: str, path: str, columns: list[str]) -> int:
    parameters = {
        "catalog": CATALOG,
        "schema": SCHEMA,
        "volume": VOLUME,
        "endpoint": ENDPOINT,
        "run_id": run_id,
        "input_path": path,
        "selected_columns_json": json.dumps(columns),
        "batch_size": str(BATCH_SIZE),
    }
    response = workspace_client().jobs.run_now(job_id=int(JOB_ID), job_parameters=parameters)
    return int(response.run_id)


def render_relationships(result: dict) -> None:
    relationships = result.get("relationships", [])
    grouped: dict[str, list[dict]] = {}
    for relationship in relationships:
        grouped.setdefault(relationship["relationship_type"], []).append(relationship)
    labels = {
        "orthographic_variant": "Spelling variants",
        "transliteration_variant": "Transliteration variants",
        "phonetic_variant": "Phonetic variants",
        "cultural_cognate": "Cultural cognates",
        "nickname": "Nicknames",
    }
    for kind, label in labels.items():
        st.markdown(f"#### {label}")
        values = grouped.get(kind, [])[:5]
        if not values:
            coverage = result.get("coverage_notes", {}).get(kind, "")
            st.caption(coverage or f"No well-attested {label.lower()} were identified.")
            continue
        for value in values:
            st.markdown(
                f"**{value['name']}** — {value['cultural_context']}  \n"
                f"{value['why']} · confidence {value['confidence']:.0%}"
            )


def render_csv_dashboard(run_id: str) -> None:
    """Render customer-facing results for one uploaded CSV."""
    run = query_frame(
        f"SELECT run_id, status, source_rows, unique_names, new_names, cached_names, "
        f"processed_names, failed_names, updated_at, error_message "
        f"FROM `{CATALOG}`.`{SCHEMA}`.`analysis_runs` WHERE run_id = ?",
        [run_id],
    )
    if run.empty:
        st.info("This CSV run is initializing. Refresh the page shortly.")
        return

    row = run.iloc[0]
    st.markdown("### CSV analysis dashboard")
    st.caption(f"Run {run_id} · status: {row['status']} · updated {row['updated_at']}")
    metrics = st.columns(4)
    metrics[0].metric("Source rows", int(row["source_rows"] or 0))
    metrics[1].metric("Unique names", int(row["unique_names"] or 0))
    metrics[2].metric("Analyzed", int(row["processed_names"] or 0))
    metrics[3].metric("Failed", int(row["failed_names"] or 0))
    cache_metrics = st.columns(2)
    cache_metrics[0].metric("New names", int(row["new_names"] or 0))
    cache_metrics[1].metric("Reused from cache", int(row["cached_names"] or 0))
    if row.get("error_message"):
        st.error(str(row["error_message"]))

    traditions = query_frame(
        f"""
        WITH run_names AS (
          SELECT DISTINCT name_hash FROM `{CATALOG}`.`{SCHEMA}`.`source_name_values` WHERE run_id = ?
        )
        SELECT coalesce(a.primary_name_tradition, 'Pending') AS name_tradition,
               count(*) AS unique_names
        FROM run_names r LEFT JOIN `{CATALOG}`.`{SCHEMA}`.`name_analysis_cache` a USING (name_hash)
        GROUP BY coalesce(a.primary_name_tradition, 'Pending')
        ORDER BY unique_names DESC LIMIT 25
        """,
        [run_id],
    )
    if not traditions.empty:
        st.caption("Name-tradition associations in this CSV—not the identity of the people named.")
        st.bar_chart(traditions.set_index("name_tradition"))

    relationship_labels = {
        "orthographic_variant": "Spelling variants",
        "transliteration_variant": "Transliterations",
        "phonetic_variant": "Phonetic variants",
        "cultural_cognate": "Cultural cognates",
        "nickname": "Nicknames",
    }
    result_tabs = st.tabs(["Name analysis", *relationship_labels.values()])

    with result_tabs[0]:
        analysis = query_frame(
            f"""
            SELECT s.normalized_name AS name, count(*) AS occurrences,
                   a.primary_name_tradition, a.confidence, a.review_required,
                   get_json_object(a.response_json, '$.name_form') AS likely_form,
                   get_json_object(a.response_json, '$.summary') AS summary,
                   get_json_object(a.response_json, '$.ambiguity_note') AS ambiguity_note
            FROM `{CATALOG}`.`{SCHEMA}`.`source_name_values` s
            LEFT JOIN `{CATALOG}`.`{SCHEMA}`.`name_analysis_cache` a USING (name_hash)
            WHERE s.run_id = ?
            GROUP BY s.normalized_name, a.primary_name_tradition, a.confidence,
                     a.review_required, a.response_json
            ORDER BY occurrences DESC, name LIMIT 50000
            """,
            [run_id],
        )
        st.dataframe(analysis, use_container_width=True, hide_index=True)
        st.download_button(
            "Download name analysis table",
            data=analysis.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"name_analysis_{run_id}.csv",
            mime="text/csv",
        )

    for panel, relationship_type in zip(result_tabs[1:], relationship_labels):
        label = relationship_labels[relationship_type]
        with panel:
            relationships = query_frame(
                f"""
                WITH run_names AS (
                  SELECT DISTINCT name_hash, normalized_name
                  FROM `{CATALOG}`.`{SCHEMA}`.`source_name_values` WHERE run_id = ?
                )
                SELECT r.normalized_name AS input_name, n.related_name,
                       n.cultural_context, n.why, n.confidence
                FROM run_names r
                INNER JOIN `{CATALOG}`.`{SCHEMA}`.`name_relationships` n USING (name_hash)
                WHERE n.relationship_type = ?
                ORDER BY input_name, n.confidence DESC LIMIT 50000
                """,
                [run_id, relationship_type],
            )
            if relationships.empty:
                st.info(f"No {label.lower()} were returned for this CSV.")
            else:
                st.dataframe(relationships, use_container_width=True, hide_index=True)
                st.download_button(
                    f"Download {label.lower()}",
                    data=relationships.to_csv(index=False).encode("utf-8-sig"),
                    file_name=f"{relationship_type}_{run_id}.csv",
                    mime="text/csv",
                    key=f"download_{relationship_type}_{run_id}",
                )


def configuration_ready() -> bool:
    missing = [
        name
        for name, value in {
            "NI_CATALOG": CATALOG,
            "NI_SCHEMA": SCHEMA,
            "NI_VOLUME": VOLUME,
            "NI_WAREHOUSE_ID": WAREHOUSE_ID,
            "NI_ENDPOINT": ENDPOINT,
            "NI_JOB_ID": JOB_ID,
        }.items()
        if not value
    ]
    if missing:
        st.error("Installation is incomplete. Missing: " + ", ".join(missing))
        return False
    return True


st.title("Name Intelligence")
st.caption("Linguistic name traditions and culturally aware relationships — not personal identity inference.")

if not configuration_ready():
    st.stop()

batch_tab, single_tab, why_tab = st.tabs(["CSV analyzer & dashboard", "Single name", "Ask why"])

with batch_tab:
    st.subheader("Analyze a CSV")
    uploaded = st.file_uploader("Upload a CSV", type=["csv"])
    if uploaded is not None:
        uploaded.seek(0)
        preview = pd.read_csv(uploaded, nrows=5000, sep=None, engine="python")
        uploaded.seek(0)
        profiles = detect_name_columns(preview)
        detected = selected_columns(profiles)

        st.write("Detected columns")
        profile_frame = pd.DataFrame([profile.as_dict() for profile in profiles])
        st.dataframe(profile_frame, use_container_width=True, hide_index=True)
        chosen = st.multiselect(
            "Columns to analyze",
            options=list(preview.columns),
            default=detected,
        )
        st.dataframe(preview.head(25), use_container_width=True, hide_index=True)

        if st.button("Upload and start processing", type="primary", disabled=not chosen):
            run_id = str(uuid.uuid4())
            with st.spinner("Uploading and starting the scalable batch job..."):
                volume_path = upload_to_volume(uploaded, run_id)
                job_run_id = start_batch_job(run_id, volume_path, chosen)
            st.success(f"Run started: {run_id}")
            st.session_state["latest_run_id"] = run_id
            st.caption(f"Databricks job run: {job_run_id}")

    try:
        recent_runs = query_frame(
            f"SELECT run_id, status, updated_at FROM `{CATALOG}`.`{SCHEMA}`.`analysis_runs` "
            "ORDER BY updated_at DESC LIMIT 25"
        )
        if recent_runs.empty:
            st.info("Upload a CSV to create the first analysis dashboard.")
        else:
            run_ids = recent_runs["run_id"].astype(str).tolist()
            preferred = st.session_state.get("latest_run_id")
            selected_index = run_ids.index(preferred) if preferred in run_ids else 0
            status_by_run = dict(zip(run_ids, recent_runs["status"].astype(str)))
            selected_run = st.selectbox(
                "CSV analysis run",
                run_ids,
                index=selected_index,
                format_func=lambda value: f"{value} · {status_by_run[value]}",
            )
            render_csv_dashboard(selected_run)
    except Exception as exc:
        st.info(f"The CSV dashboard is initializing: {exc}")

with single_tab:
    st.subheader("Explore one name")
    name = st.text_input("Name", placeholder="Example: Mohammad")
    context = st.text_input("Optional context", placeholder="Example: Arabic-speaking context")
    refresh = st.checkbox(
        "Refresh with a new Llama 70B analysis",
        help="Bypass the saved single-name result and replace it with a fresh analysis.",
    )
    if st.button("Analyze name", type="primary", disabled=not name.strip()):
        normalized = normalize_name(name)
        cached = pd.DataFrame()
        if not refresh:
            cached = query_frame(
                f"SELECT response_json FROM `{CATALOG}`.`{SCHEMA}`.`single_name_analysis_cache` "
                "WHERE cache_key = ? ORDER BY processed_at DESC LIMIT 1",
                [single_cache_key(normalized, context)],
            )
        if not cached.empty:
            result = json.loads(cached.iloc[0]["response_json"])
            result["input_name"] = normalized
            st.caption("Returned from the detailed single-name cache.")
        else:
            with st.spinner("Building a detailed cultural and linguistic analysis..."):
                result = model_query([normalized], context, rich_single=True)[0]
                cache_single_result(normalized, context, result)

        st.markdown(f"### {result['input_name']}")
        left, right, third = st.columns(3)
        left.metric("Likely form", result.get("name_form", "unknown").replace("_", " ").title())
        right.metric("Primary tradition", result.get("primary_name_tradition", "Unknown"))
        third.metric("Confidence", f"{result.get('confidence', 0):.0%}")
        st.info(result.get("summary", "No summary returned."))
        if result.get("meaning_and_etymology"):
            st.markdown("#### Meaning and etymology")
            st.write(result["meaning_and_etymology"])
        if result.get("cultural_usage"):
            st.markdown("#### Cultural usage")
            st.write(result["cultural_usage"])
        if result.get("pronunciation_note"):
            st.markdown("#### Pronunciation")
            st.write(result["pronunciation_note"])
        if result.get("ambiguity_note"):
            st.warning(result["ambiguity_note"])
        render_relationships(result)
        st.session_state["current_analysis"] = result

with why_tab:
    st.subheader("Ask why")
    current = st.session_state.get("current_analysis")
    if not current:
        st.info("Analyze or select a name first. The answer will be grounded in that saved analysis.")
    else:
        st.json(current, expanded=False)
        question = st.text_area("Question", placeholder="Why was Muhammad linked to Mohammad?")
        if st.button("Answer from this analysis", disabled=not question.strip()):
            grounded_context = json.dumps(current, ensure_ascii=False)
            prompt = (
                "Answer only from the supplied analysis. Explain uncertainty and never claim a person's identity. "
                "If the analysis does not support the answer, say so.\n"
                f"ANALYSIS={grounded_context}\nQUESTION={question}"
            )
            response = workspace_client().api_client.do(
                "POST",
                f"/serving-endpoints/{ENDPOINT}/invocations",
                body={
                    "messages": [
                        {"role": "system", "content": "You explain stored onomastic analysis cautiously and concisely."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 600,
                },
            )
            st.write(response["choices"][0]["message"]["content"])
