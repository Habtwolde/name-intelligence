from __future__ import annotations

import io
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
from name_intelligence.prompting import PROMPT_VERSION, SYSTEM_PROMPT, build_user_prompt, response_schema
from name_intelligence.validation import validate_analysis


st.set_page_config(page_title="Name Intelligence", page_icon="◈", layout="wide")


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


CATALOG = env("NI_CATALOG")
SCHEMA = env("NI_SCHEMA", "name_intelligence")
VOLUME = env("NI_VOLUME", "files")
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


def model_query(names: list[str], context: str = "") -> list[dict]:
    payload = {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(names, context)},
        ],
        "temperature": 0.1,
        "max_tokens": max(1200, 750 * len(names)),
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "name_analysis", "strict": True, "schema": response_schema()},
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


def cache_single_result(normalized: str, result: dict) -> None:
    response_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    execute(
        f"""
        MERGE INTO `{CATALOG}`.`{SCHEMA}`.`name_analysis_cache` t
        USING (SELECT ? AS name_hash, ? AS normalized_name) s
        ON t.name_hash=s.name_hash
        WHEN MATCHED THEN UPDATE SET
          normalized_name=s.normalized_name, response_json=?, primary_name_tradition=?, confidence=?,
          review_required=?, status='SUCCESS', model_endpoint=?, prompt_version=?,
          processed_at=current_timestamp(), error_message=NULL
        WHEN NOT MATCHED THEN INSERT (
          name_hash, normalized_name, family_id, response_json, primary_name_tradition, confidence,
          review_required, status, model_endpoint, prompt_version, processed_at, error_message
        ) VALUES (s.name_hash, s.normalized_name, NULL, ?, ?, ?, ?, 'SUCCESS', ?, ?, current_timestamp(), NULL)
        """,
        [
            stable_name_hash(normalized), normalized,
            response_json, result.get("primary_name_tradition", "Unknown"), float(result.get("confidence", 0)),
            bool(result.get("review_required", False)), ENDPOINT, ACTIVE_PROMPT_VERSION,
            response_json, result.get("primary_name_tradition", "Unknown"), float(result.get("confidence", 0)),
            bool(result.get("review_required", False)), ENDPOINT, ACTIVE_PROMPT_VERSION,
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
            st.caption("No sufficiently reliable result returned.")
            continue
        for value in values:
            st.markdown(
                f"**{value['name']}** — {value['cultural_context']}  \n"
                f"{value['why']} · confidence {value['confidence']:.0%}"
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

batch_tab, single_tab, why_tab, dashboard_tab = st.tabs(
    ["Batch analyzer", "Single name", "Ask why", "Dashboard"]
)

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

    latest_run = st.session_state.get("latest_run_id")
    if latest_run:
        try:
            status = query_frame(
                f"SELECT run_id, status, source_rows, unique_names, new_names, cached_names, processed_names, failed_names, updated_at "
                f"FROM `{CATALOG}`.`{SCHEMA}`.`analysis_runs` WHERE run_id = ?",
                [latest_run],
            )
            st.dataframe(status, use_container_width=True, hide_index=True)
        except Exception as exc:
            st.info(f"The run is initializing: {exc}")

with single_tab:
    st.subheader("Explore one name")
    name = st.text_input("Name", placeholder="Example: Mohammad")
    context = st.text_input("Optional context", placeholder="Example: Arabic-speaking context")
    if st.button("Analyze name", type="primary", disabled=not name.strip()):
        normalized = normalize_name(name)
        cached = query_frame(
            f"SELECT response_json FROM `{CATALOG}`.`{SCHEMA}`.`name_analysis_cache` "
            "WHERE name_hash = ? AND status = 'SUCCESS' AND model_endpoint = ? AND prompt_version = ? "
            "ORDER BY processed_at DESC LIMIT 1",
            [stable_name_hash(normalized), ENDPOINT, ACTIVE_PROMPT_VERSION],
        )
        if not cached.empty:
            result = json.loads(cached.iloc[0]["response_json"])
            result["input_name"] = normalized
            st.caption("Returned from the reusable analysis cache.")
        else:
            with st.spinner("Analyzing linguistic name associations..."):
                result = model_query([normalized], context)[0]
                cache_single_result(normalized, result)

        st.markdown(f"### {result['input_name']}")
        left, right, third = st.columns(3)
        left.metric("Likely form", result.get("name_form", "unknown").replace("_", " ").title())
        right.metric("Primary tradition", result.get("primary_name_tradition", "Unknown"))
        third.metric("Confidence", f"{result.get('confidence', 0):.0%}")
        st.info(result.get("summary", "No summary returned."))
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

with dashboard_tab:
    st.subheader("Processing overview")
    try:
        summary = query_frame(
            f"SELECT count(*) AS analyzed_names, "
            f"sum(CASE WHEN review_required THEN 1 ELSE 0 END) AS review_required, "
            f"avg(confidence) AS average_confidence "
            f"FROM `{CATALOG}`.`{SCHEMA}`.`name_analysis_cache` WHERE status = 'SUCCESS'"
        )
        if not summary.empty:
            a, b, c = st.columns(3)
            a.metric("Analyzed names", int(summary.iloc[0]["analyzed_names"] or 0))
            b.metric("Needs review", int(summary.iloc[0]["review_required"] or 0))
            c.metric("Average confidence", f"{float(summary.iloc[0]['average_confidence'] or 0):.0%}")
        traditions = query_frame(
            f"SELECT primary_name_tradition, count(*) AS names FROM `{CATALOG}`.`{SCHEMA}`.`name_analysis_cache` "
            f"WHERE status = 'SUCCESS' GROUP BY primary_name_tradition ORDER BY names DESC LIMIT 20"
        )
        if not traditions.empty:
            st.caption("Distribution of name-tradition associations, not people by culture.")
            st.bar_chart(traditions.set_index("primary_name_tradition"))
        recent = query_frame(
            f"SELECT run_id, status, source_rows, unique_names, processed_names, failed_names, updated_at "
            f"FROM `{CATALOG}`.`{SCHEMA}`.`analysis_runs` ORDER BY updated_at DESC LIMIT 25"
        )
        st.dataframe(recent, use_container_width=True, hide_index=True)
        if not recent.empty:
            chosen_run = st.selectbox("Inspect a completed run", recent["run_id"].astype(str).tolist())
            preview = query_frame(
                f"SELECT s.source_column, s.original_name, s.normalized_name, a.primary_name_tradition, "
                f"a.confidence, a.review_required, a.response_json FROM `{CATALOG}`.`{SCHEMA}`.`source_name_values` s "
                f"LEFT JOIN `{CATALOG}`.`{SCHEMA}`.`name_analysis_cache` a USING (name_hash) "
                "WHERE s.run_id = ? LIMIT 50000",
                [chosen_run],
            )
            st.dataframe(preview.head(500), use_container_width=True, hide_index=True)
            st.download_button(
                "Download up to 50,000 displayed-run values",
                data=preview.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"name_intelligence_{chosen_run}.csv",
                mime="text/csv",
            )
            st.caption(
                f"The complete partitioned export is stored at "
                f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/exports/{chosen_run}."
            )
    except Exception as exc:
        st.warning(f"Dashboard data is not available yet: {exc}")
