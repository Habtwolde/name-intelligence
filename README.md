# Name Intelligence for Databricks

A portable Databricks App for scalable, culturally aware analysis of written
names. It identifies likely linguistic/name traditions, distinguishes variants
from cognates and nicknames, explains every relationship, supports grounded
follow-up questions, and never claims that a name proves a person's identity.

## What the package does

- Detects name-bearing CSV columns from their values; headers are optional hints.
- Preserves multi-token names, non-Latin scripts, diacritics, apostrophes and hyphens.
- Uses Spark for ingestion, unpivoting, normalization and deduplication.
- Sends only unresolved unique names to the model endpoint.
- Batches up to 25 names per request to amortize prompt overhead.
- Reuses exact results and high-confidence name-family members.
- Processes high-frequency names first and caps new names per run.
- Saves every result, explanation, failure and model/prompt version in Delta.
- Exports enriched row-level results back to the managed project volume.
- Offers batch upload, single-name lookup, grounded “Ask why,” and a dashboard.

## Install in a client workspace

1. Import the extracted `name-intelligence` directory into a Databricks Workspace folder.
2. Open `notebooks/00_RUN_ME_INSTALL_AND_DEPLOY`.
3. Review the configuration widgets at the top. `AUTO` is appropriate for most fields.
4. Run all cells.

The notebook discovers the workspace ID, workspace host, authenticated user,
current catalog, accessible warehouses and model-serving endpoints. It then
creates the schema, volume, Delta tables, batch job and app; grants the app's
dedicated service principal the required access; deploys the app; and runs the
bundled acceptance test.

No Catalog Explorer setup is part of the installation.

### Prerequisites

The identity running the installer must be allowed to:

- Create a schema and managed volume in the selected catalog.
- Create and manage Lakeflow Jobs and Databricks Apps.
- Use and grant access to a SQL warehouse and model-serving endpoint.
- Grant privileges on the project objects it creates.

The preflight/install cells stop with a specific error if workspace policy
does not permit one of these actions. The installer does not circumvent policy.

## Configuration

| Widget | Default | Behavior |
|---|---:|---|
| `target_catalog` | `AUTO` | Uses the notebook's current catalog. |
| `target_schema` | `name_intelligence` | Created if missing. |
| `warehouse_id` | `AUTO` | Selects an accessible running warehouse. |
| `endpoint_name` | `databricks-meta-llama-3-3-70b-instruct` | The installer enforces this Llama 3.3 70B endpoint. |
| `auto_create_warehouse` | `false` | When enabled, creates a small serverless warehouse only if none exists. |
| `batch_size` | `20` | Names per endpoint request; allowed range 1–25. |
| `max_concurrent_requests` | `1` | Endpoint calls in flight; kept at one to stay within pay-per-token output quotas. |
| `max_new_names_per_run` | `100000` | Cost circuit breaker. |
| `run_acceptance_test` | `true` | Runs the included 30-row test file. |

The installer attaches the selected warehouse, endpoint, job, and volume as
Databricks App resources. `app.yaml` resolves their workspace-specific values at
runtime, so the source code never contains another environment's workspace,
warehouse, job, application, volume, or service-principal ID.

## Million-row cost controls

Let `R` be source rows, `U` unique normalized names, `C` exact/family cache hits,
and `B` names per model request. Approximate new calls are:

```text
ceil((U - C) / B)
```

The pipeline never invokes the LLM for all `R` rows. It also:

- Orders unresolved names by source frequency.
- Stops at the configured new-name ceiling.
- Retries transient endpoint failures up to four times.
- Flushes results incrementally so an interruption does not discard completed work.
- Reuses family templates only for members recorded at confidence 0.85 or higher.
- Generates short stored rationales once; follow-up chat is invoked only on demand.

## Output semantics

The application separates:

- `orthographic_variant`: another spelling in the same writing tradition.
- `transliteration_variant`: another Romanization or script representation.
- `phonetic_variant`: a likely same/near-same pronunciation in the stated context.
- `cultural_cognate`: a related form sharing an etymological origin.
- `nickname`: a culture-specific diminutive or informal form.

Each category is limited to five entries. Arrays may be empty. Low-confidence,
ambiguous, corrupted and single-letter values are flagged for review rather
than completed with invented content.

## Project layout

```text
name-intelligence/
├── app.yaml
├── requirements.txt
├── sample_names_test.csv
├── app/app.py
├── config/defaults.yaml
├── notebooks/
│   ├── 00_RUN_ME_INSTALL_AND_DEPLOY.py
│   ├── 01_BATCH_NAME_PIPELINE.py
│   ├── 02_RETRY_AND_VALIDATE.py
│   └── 03_ACCEPTANCE_TESTS.py
├── src/name_intelligence/
└── tests/
```

## Operational notes

- The full enriched output is written to
  `/Volumes/<catalog>/<schema>/<volume>/exports/<run_id>` as partitioned CSV.
- The browser UI intentionally paginates and aggregates; it does not load a
  million-row result into application memory.
- Re-running an existing `run_id` replaces that run's staged rows and skips
  already-successful cached names.
- The required `databricks-meta-llama-3-3-70b-instruct` endpoint must be
  available in the target workspace region and queryable by the installer.
- Names are personal data. Use appropriate workspace access, retention and
  client governance rules. Do not use name-tradition associations to make
  employment, eligibility, fraud, credit, healthcare or similar decisions.

