## Plan: Swedish Exercise Book Generator — Full Recommended Path

**TL;DR**: Build end-to-end in two phases. Phase 1 is a fully working local system (DuckDB + Dagster + Gemini + Streamlit) that you can ship and learn from in weeks. Phase 2 lifts it to production GCP (BigQuery + GCS + Cloud Run + Next.js) without rewriting the logic — only swapping the I/O layer. The Dagster asset graph and AI agent code are written once and work in both phases.

---

## Repository Layout (final target)

```
exercise-book-generator/
├── dagster_pipeline/
│   ├── assets/
│   │   ├── bronze.py          # RSS → raw storage
│   │   ├── silver.py          # clean, dedup, validate
│   │   └── gold.py            # AI agent → exercise sets
│   ├── agents/
│   │   ├── exercise_agent.py  # Gemini caller + Pydantic schemas
│   │   └── prompts/           # versioned prompt templates (one per exercise type)
│   │       ├── comprehension.txt
│   │       ├── vocabulary.txt
│   │       ├── grammar.txt
│   │       ├── multiple_choice.txt
│   │       └── summary.txt
│   ├── resources/
│   │   ├── storage.py         # I/O abstraction (DuckDB local ↔ BigQuery/GCS)
│   │   └── llm.py             # Gemini client wrapper
│   ├── checks/
│   │   └── quality_checks.py  # Dagster asset checks
│   ├── sensors/
│   │   └── rss_sensor.py      # Dagster sensor to detect new RSS items
│   └── definitions.py         # Dagster Definitions root
├── api/                       # FastAPI (Phase 2 → Cloud Run)
│   ├── main.py
│   └── routers/exercises.py
├── frontend/                  # Streamlit (Phase 1) / Next.js (Phase 2)
│   └── app.py                 # Phase 1 Streamlit
├── infra/                     # Terraform (Phase 2)
│   ├── main.tf
│   ├── bigquery.tf
│   ├── gcs.tf
│   └── cloudrun.tf
├── tests/
│   ├── test_silver.py
│   ├── test_agent.py
│   └── fixtures/sample_article.json
├── pyproject.toml             # uv project file
├── .env.example
└── docker-compose.yml         # local Dagster + optional services
```

---

## Phase 1 — Local Prototype (weeks 1–4)

**Goal**: Full pipeline running locally. Real articles → real exercises → interactive Streamlit app. No cloud infra costs.

### Step 1 · Project scaffold

- Init Python project with `uv` (`uv init`, `uv add dagster dagster-webserver`)
- Create the folder structure above (empty files)
- `.env.example` with `GEMINI_API_KEY`, `ENVIRONMENT=local`
- `pyproject.toml` dependencies: `dagster`, `dagster-duckdb`, `feedparser`, `google-generativeai`, `pydantic`, `langdetect`, `streamlit`, `beautifulsoup4`

### Step 2 · Storage abstraction (`resources/storage.py`)

- Define a `StorageResource` Dagster resource with two implementations:
  - `DuckDBStorage` — reads/writes local `.duckdb` file, exposes `execute(sql)` and `write_df(table, df)`
  - `BigQueryStorage` (Phase 2 stub, raises `NotImplementedError` for now)
- This is the key architectural decision that makes Phase 1 → Phase 2 a swap, not a rewrite
- Configure via `dagster_pipeline/definitions.py` using `EnvVar("ENVIRONMENT")`

### Step 3 · Bronze asset (`assets/bronze.py`)

- Dagster asset `rss_raw_articles`, partitioned by `DailyPartitionsDefinition`
- Sources list hardcoded: SVT Nyheter, DN, Aftonbladet RSS URLs
- Uses `feedparser` to fetch each feed
- Stores raw items as JSON to `data/bronze/{source}/{date}.json` (local) or GCS `raw/` (Phase 2)
- **Asset metadata**: emit `dagster.Output` with `metadata={"num_articles": len(items), "sources": sources}`
- **No transformation here** — Bronze = raw, immutable. Even malformed articles are kept.

### Step 4 · Silver asset (`assets/silver.py`)

- Dagster asset `silver_articles`, depends on `rss_raw_articles`
- Transformations (in order):
  1. Parse HTML out of article summaries (`BeautifulSoup`)
  2. Detect language with `langdetect` → filter keep only `sv` (Swedish)
  3. Deduplicate by SHA-256 hash of the article URL
  4. Drop articles with < 150 words
  5. Normalise schema: `{url, title, body, source, published_at, ingested_at, word_count}`
- Write to DuckDB table `silver_articles` (partitioned column `ingested_date`)
- **Dagster Asset Check** `check_silver_quality`: assert language purity > 95 %, null rate on `title` = 0

### Step 5 · AI Agent (`agents/exercise_agent.py`)

- `ExerciseAgent` class with single public method: `generate(article: Article) -> ExerciseSet`
- Uses `google-generativeai` with `gemini-1.5-flash`
- JSON mode enforced via `generation_config={"response_mime_type": "application/json"}`
- **Pydantic schemas** (strict, validated):
  ```
  ExerciseSet { article_url, exercises: list[Exercise] }
  Exercise    { type: ExerciseType, question, answer, options?: list[str] }
  ExerciseType: Enum [comprehension, vocabulary, grammar, multiple_choice, summary_prompt]
  ```
- Each exercise type has its own prompt in `agents/prompts/*.txt` — loaded at runtime
- Retry logic: max 3 attempts with exponential backoff if Pydantic validation fails
- **Critical pattern**: Pydantic validates the LLM output, not just `json.loads()`. If schema is wrong, retry not just parse error.

### Step 6 · Gold asset (`assets/gold.py`)

- Dagster asset `gold_exercise_sets`, depends on `silver_articles`
- Reads Silver rows not yet in Gold (anti-join on `article_url`)
- Calls `ExerciseAgent.generate()` for each new article
- Writes resulting `ExerciseSet` rows to DuckDB table `gold_exercise_sets`
- Schema: `{id, article_url, exercise_type, question, answer, options_json, created_at, prompt_version}`
- Store `prompt_version` (hash of prompt file) so you can re-generate if prompt changes
- **Asset metadata**: emit `num_exercises_generated`, `avg_generation_time_s`

### Step 7 · Dagster sensor (`sensors/rss_sensor.py`)

- `rss_freshness_sensor`: runs hourly, checks if Bronze partition for today has been materialized
- If not → triggers `rss_raw_articles` run for today's partition
- This is how new articles flow in automatically without Cloud Scheduler in Phase 1

### Step 8 · Streamlit app (`frontend/app.py`)

- Connects directly to DuckDB file
- Pages:
  - **Browse articles**: list Silver articles with word count + source filter
  - **Exercise session**: pick an article → show all 5 exercise types as interactive cards
  - **Comprehension / vocab / grammar**: text input + check answer button (compare to `answer` field)
  - **Multiple choice**: radio buttons with immediate feedback
  - **Summary prompt**: text area (no auto-grading, just reference answer shown on submit)
- Session state in `st.session_state` (no DB writes needed in Phase 1)

### Step 9 · Testing

- `tests/test_silver.py`: unit test each transformation function with `fixtures/sample_article.json`
- `tests/test_agent.py`: mock Gemini call, assert Pydantic schema validates correctly, assert retry fires on bad JSON
- Run with `pytest`

### Step 10 · Phase 1 verification

- `dagster dev` → open localhost:3000, see full asset graph
- Materialize all assets end-to-end
- Confirm DuckDB has rows in all three tables
- Run Streamlit: `streamlit run frontend/app.py`
- Complete one full exercise session on a real SVT article

---

## Phase 2 — GCP Production (weeks 5–10)

**Goal**: Swap DuckDB → BigQuery/GCS, no logic changes. Add FastAPI + Next.js + Cloud Run + Terraform.

### Step 11 · GCP setup

- Create GCP project `exercise-book-generator`
- Enable APIs: BigQuery, Cloud Storage, Cloud Run, Secret Manager, Cloud Build, Cloud Scheduler
- Create service account `dagster-runner` with roles: `bigquery.dataEditor`, `storage.objectAdmin`, `secretmanager.secretAccessor`
- Store `GEMINI_API_KEY` in Secret Manager
- Set `$10/month` billing alert

### Step 12 · Terraform infra (`infra/`)

- `gcs.tf`: three buckets — `raw-articles/` (Bronze), `silver-parquet/` (Silver backup), `dagster-io/` (Dagster I/O manager)
- `bigquery.tf`: dataset `exercise_book`, tables `articles_raw`, `silver_articles` (partitioned by `ingested_date`), `gold_exercise_sets` (partitioned by `created_date`)
- `cloudrun.tf`: Cloud Run service for FastAPI, min instances = 0 (cold start OK for Phase 2)
- Table schemas defined in Terraform as JSON schema files — version controlled

### Step 13 · Swap StorageResource

- Implement `BigQueryStorage` in `resources/storage.py` using `google-cloud-bigquery` SDK
- `GCSBronzeStorage` for raw JSON: reads/writes blobs with `google-cloud-storage`
- In `definitions.py`: switch resource based on `EnvVar("ENVIRONMENT")` — `local` → DuckDB, `production` → BigQuery
- **No changes to `bronze.py`, `silver.py`, `gold.py`** — they only call `context.resources.storage`

### Step 14 · Deploy Dagster to Cloud Run (or Cloud Composer lite)

- Option A (simpler): Dagster Cloud free tier — point to your GCP resources, free orchestration
- Option B (self-hosted): Dagster as a Cloud Run service + Cloud SQL (Postgres) for run storage
- Recommended: **Dagster Cloud** (free tier handles solo projects, no infra to manage)
- Update sensor: replace local file check with GCS bucket existence check

### Step 15 · Cloud Scheduler trigger

- Schedule: `0 7 * * *` (7am Stockholm time daily)
- Triggers Dagster Cloud webhook → materializes `rss_raw_articles` → downstream cascade

### Step 16 · FastAPI (`api/`)

- `GET /exercises` — query params: `source`, `topic`, `type`, `limit`
- `GET /exercises/{exercise_id}` — single exercise with article metadata
- `POST /sessions/{session_id}/answer` — record answer (right/wrong) to BigQuery `user_answers` table
- `GET /articles` — browse Silver articles
- Auth: API key via header `X-API-Key` (Secret Manager), no user auth needed for Phase 2
- Containerise with `Dockerfile` (Python slim), deploy to Cloud Run

### Step 17 · Next.js frontend (`frontend/` — new subfolder)

- Replace Streamlit with Next.js App Router
- Pages: `/` (home), `/exercises` (browse + filter), `/exercises/[id]` (interactive session)
- Calls FastAPI via `NEXT_PUBLIC_API_URL` env var
- Deploy to **Firebase Hosting** (free tier, global CDN, integrates with GCP project)
- Same exercise UI as Streamlit but persistent, shareable URLs

### Step 18 · CI/CD (`cloudbuild.yaml` + GitHub Actions)

- On push to `main`:
  - Run `pytest`
  - Build + push FastAPI Docker image to Artifact Registry
  - Deploy to Cloud Run with `gcloud run deploy`
  - Deploy Next.js to Firebase Hosting with `firebase deploy`

### Step 19 · Monitoring

- Dagster asset freshness checks: alert if Silver not updated in 25 hours
- Cloud Monitoring: Cloud Run error rate alert (> 1% → email)
- BigQuery: scheduled query runs nightly counting exercises per source — stored in `monitoring_daily` table
- **Data quality dashboard**: Streamlit (or Metabase free) pointing at BigQuery `monitoring_daily`

### Step 20 · Phase 2 verification

- `terraform apply` — all resources created
- Manual trigger of Dagster materialization from Cloud Scheduler
- BigQuery tables populated: check in BQ console
- `curl https://your-cloud-run-url/exercises` returns JSON
- Next.js app live on Firebase Hosting, full exercise session works

---

## Phase 3 — Optional Enhancements (after Phase 2 is stable)

These are independent and can be picked in any order:

**3a. Pub/Sub event-driven ingestion** — Cloud Scheduler → Pub/Sub topic → Cloud Run function that pushes to Bronze GCS → triggers Dagster sensor immediately. Near-real-time instead of daily batch.

**3b. dbt for Silver transformations** — replace `silver.py` SQL with dbt models. Adds lineage docs, `dbt test`, and a data catalog. Use `dagster-dbt` integration so Dagster orchestrates dbt runs.

**3c. Feedback loop** — `POST /exercises/{id}/feedback` endpoint → write to `user_feedback` BigQuery table → weekly Dagster job reads bad-rated exercises → re-runs Gemini agent with improved prompt.

**3d. Difficulty scoring** — Gold asset adds `difficulty_score` (Gemini prompt: "rate this exercise 1-5 for a B1 Swedish learner"). Filter in web app by difficulty.

---

## Relevant Files

- .gitignore — already exists, will be extended
- `dagster_pipeline/definitions.py` — Dagster entry point, resource config
- `dagster_pipeline/resources/storage.py` — the key abstraction enabling Phase 1→2 swap
- `dagster_pipeline/agents/exercise_agent.py` — core AI logic, written once
- `dagster_pipeline/assets/silver.py` — most complex transformation, most unit tested
- `infra/bigquery.tf` — table schemas, partition specs
- `api/main.py` — FastAPI root
- `pyproject.toml` — all dependencies

---

## Verification Checklist

**Phase 1**

1. `dagster dev` — asset graph renders bronze → silver → gold with no errors
2. `dagster asset materialize --select "*"` on sample RSS data completes without failures
3. DuckDB has > 0 rows in all three tables
4. Dagster asset check `check_silver_quality` passes (> 95% Swedish)
5. `streamlit run frontend/app.py` — complete a full exercise session

**Phase 2** 6. `terraform plan` produces no errors, `terraform apply` succeeds 7. BigQuery tables `silver_articles` and `gold_exercise_sets` have data after first Cloud Scheduler trigger 8. `pytest tests/` passes in CI (Cloud Build) 9. FastAPI `/exercises` endpoint responds in < 200ms (BigQuery cache warm) 10. Next.js app deployed on Firebase, accessible via public URL

---

## Decisions

- **`uv` over `pip/poetry`**: faster, lockfile is reproducible, modern standard
- **DuckDB in Phase 1**: zero cloud cost during dev; same SQL dialect as BQ for most operations
- **Dagster over Airflow**: asset-centric model shows lineage natively; local dev with `dagster dev` is instant; no Kubernetes needed
- **Gemini 1.5 Flash over Pro**: 10x cheaper, sufficient for structured JSON extraction; Pro reserved for fallback if quality is poor
- **Firebase Hosting over Cloud Run for frontend**: static export, global CDN, free tier, no cold starts
- **Excluded from scope**: user auth/login, multi-language support, mobile app, fine-tuning models, Dataflow/Beam
