## Plan: Swedish EPUB Exercise Book Generator — Enterprise Pipeline

**TL;DR**: Extend the existing Dagster project into a full medallion pipeline — raw EPUBs → parsed paragraphs → LLM-generated exercises → served via a clean API. Use dbt for SQL transformations and Instructor for structured LLM output. The reader navigates book → chapter → paragraph and solves exercises attached to each paragraph.

---

### Architecture Overview

```
EPUB Files (GCS)
        ↓
   [Bronze Layer]  — raw file registry, no transformation
        ↓
   [Silver Layer]  — parsed: books → chapters → paragraphs
        ↓
   [Gold Layer]    — LLM exercises + NLP enrichment + embeddings
        ↓
   [Serving Layer] — PostgreSQL → FastAPI → Consumer App
```

---

### Phase 1 — Object Storage + Ingestion

**Goal**: Store raw EPUBs durably, register them in the catalog.

**Step 1** — Use **GCS** for all EPUB storage (dev and prod). EPUBs are uploaded out-of-band (manual or via upload endpoint later). No local object storage.

**Step 2** — Bronze Dagster asset: polls GCS for new EPUB files, registers metadata in **BigQuery**:

- table `bronze.epub_registry`: `(file_id STRING, filename STRING, storage_path STRING, sha256_hash STRING, uploaded_at TIMESTAMP, status STRING)`
- Hash-based deduplication so re-uploads are idempotent
- Configurable via `BigQueryStorage` resource

**Step 3** — Use `dagster-gcp` integration for GCS I/O and BigQuery

---

### Phase 2 — Silver: EPUB Parsing Pipeline

**Goal**: Parse EPUBs into clean, structured relational tables.

**Step 4** — Add **ebooklib + lxml + BeautifulSoup4** (already in deps) for EPUB parsing

**Step 5** — Silver assets (Dagster, partitioned by `file_id`):

- `silver.books` — title, author, language, ISBN, publication year, storage_path
- `silver.chapters` — book_id, chapter_order, title, raw_html
- `silver.paragraphs` — chapter_id, paragraph_order, text, word_count, language_code, sentence_count

Use **Polars** (faster than Pandas, columnar) for all DataFrame transformations.

**Step 6** — Paragraph segmentation strategy:

- Split on `<p>` tags from HTML chapters
- Filter: min 20 words, max 500 words (configurable)
- Language detection with **langdetect** (already in deps) to skip non-Swedish paragraphs
- Strip boilerplate (page numbers, headers, copyright notices) via heuristics

**Step 7** — **dbt** models on top of BigQuery for Silver → tested, documented SQL transforms with lineage

**Step 8** — Dagster **asset checks** for data quality:

- No empty paragraphs
- Language is `sv` (Swedish)
- Paragraph count reasonable per chapter

---

### Phase 3 — Gold: LLM Exercise Generation

**Goal**: Use LLMs to generate diverse, high-quality Swedish exercises per paragraph.

**Step 9** — **Instructor** library (wraps OpenAI/Anthropic/Gemini) for **structured output** via Pydantic models. LiteLLM for provider-agnostic switching.

**Step 10** — Exercise types (Swedish exercise book style):

- `fill_in_blank` — key word masked, multiple correct options
- `multiple_choice` — comprehension question with 4 options
- `true_false` — factual statement about paragraph
- `vocabulary` — define/use a word from context
- `summary` — write a 1-2 sentence summary prompt
- `grammar` — identify/correct a grammatical construct

**Step 11** — Gold assets:

- `gold.exercises`: exercise_id, paragraph_id, exercise_type, question_sv, answer, distractors (JSON), difficulty (1–5), generated_by_model, generated_at
- `gold.vocabulary`: word, definition_sv, example_sentence, paragraph_id
- Difficulty scoring: estimated by LLM + word rarity (TF-IDF over corpus)

**Step 12** — LLM batching with **Ray** (local Ray cluster or Ray remote) to parallelize generation across paragraphs without rate-limiting issues. Fallback: simple async batching with `asyncio` + semaphore.

**Step 13** — Retry logic + cost tracking: log token usage per asset run in `gold.llm_run_log`

---

### Phase 4 — Exercise Sets

**Goal**: Curate balanced exercise sets per chapter for the consumer app.

**Step 14** — dbt Gold model: `gold.exercise_sets` — curated sets of exercises per chapter (mix of types, balanced difficulty)

---

### Phase 5 — Serving Layer

**Goal**: Stable, versioned data contract for the consumer app.

**Step 15** — **PostgreSQL** as the serving database (better for concurrent reads, transactions, JSONB):

- Mirror Gold tables into Postgres via Dagster asset

**Step 16** — **FastAPI** service (separate repo or `api/` subdirectory):

- `GET /books` — list books
- `GET /books/{id}/chapters` — chapters
- `GET /chapters/{id}/paragraphs` — paragraphs with text
- `GET /paragraphs/{id}/exercises` — all exercises for a paragraph
- `GET /exercise-sets/{chapter_id}` — curated exercise set

---

### Tool Stack Summary

| Layer           | Tool                                 | Role                                  |
| --------------- | ------------------------------------ | ------------------------------------- |
| Orchestration   | **Dagster**                          | Asset graph, scheduling, partitioning |
| Object Storage  | **GCS**                              | EPUB file storage                     |
| Analytics DB    | **BigQuery**                         | Bronze/Silver/Gold tables             |
| Transforms      | **dbt-core**                         | SQL models with tests + lineage       |
| DataFrames      | **Polars**                           | Fast columnar transforms              |
| EPUB Parsing    | **ebooklib + lxml**                  | Chapter/paragraph extraction          |
| LLM Calls       | **Instructor + LiteLLM**             | Structured exercise generation        |
| LLM Provider    | **Gemini / GPT-4o / Claude**         | Swappable via LiteLLM                 |
| Distributed LLM | **Ray** (optional)                   | Parallel batch inference              |
| Serving DB      | **PostgreSQL**                       | Consumer app backend                  |
| API             | **FastAPI**                          | REST interface for consumer app       |
| Data Quality    | **Dagster asset checks + dbt tests** | Pipeline validation                   |

---

### Relevant Existing Files

- definitions.py — resource wiring, extend for new resources
- storage.py — add `ObjectStorageResource`, `PostgresResource`
- assets.py — split into `bronze.py`, `silver.py`, `gold.py`, `serving.py`
- pyproject.toml — add new deps
- plans — currently empty, document architectural decisions here

---

### Repo Structure (Target)

```
exercise-book-generator/
├── dagster_project/          # Core data pipeline
│   ├── src/dagster_project/
│   │   ├── defs/
│   │   │   ├── bronze.py     # EPUB registry asset
│   │   │   ├── silver.py     # Parsing assets (books/chapters/paragraphs)
│   │   │   ├── gold.py       # LLM exercise generation assets
│   │   │   └── serving.py    # Postgres mirror
│   │   ├── resources/
│   │   │   ├── storage.py    # BigQuery resource
│   │   │   ├── object_store.py  # GCS resource
│   │   │   └── llm.py        # Instructor + LiteLLM resource
│   │   └── lib/
│   │       ├── epub_parser.py   # ebooklib EPUB → paragraphs
│   │       └── exercise_schemas.py  # Pydantic models for LLM output
│   ├── dbt/                  # dbt project for SQL transforms
│   └── docker-compose.yml    # Postgres (dev)
└── api/                      # FastAPI serving layer (separate app)
```

---

### Verification Steps

1. Dagster UI shows asset graph with Bronze → Silver → Gold → Serving lineage
2. A single EPUB materialises all downstream assets without errors
3. `gold.exercises` contains all 6 exercise types with valid JSON distractors
4. FastAPI `/exercise-sets/{chapter_id}` returns a balanced set in < 200ms
5. dbt tests pass (`dbt test`) on Silver + Gold models
6. Duplicate EPUB upload produces no new rows (idempotency check)
7. GCS bucket is reachable and BigQuery dataset exists with correct permissions

---

### Decisions / Deliberately Excluded

- **Spark**: Excluded — Polars + BigQuery handle book-scale data efficiently without the operational complexity of a Spark cluster. Ray covers distributed LLM batching if needed.
- **Kafka**: Excluded — EPUB ingestion is batch, not streaming. Can add later if near-real-time exercise generation is needed.
- **Existing news scraper**: Kept alongside — paragraphs from news can also feed into exercises (richer corpus), same Silver schema.
- **Consumer app UI**: Out of scope here — FastAPI API layer is the contract boundary.
- **Qdrant / vector search**: Excluded — exercises are directly tied to paragraphs via FK; navigation is chapter → paragraph → exercises, not open-ended search. BigQuery `VECTOR_SEARCH()` available if needed later without extra infrastructure.
- **Apache Iceberg**: Excluded — native BigQuery tables (partitioned + clustered) are simpler, cheaper, and fully sufficient for this workload.

---

**Further Considerations**

1. **LLM Provider**: Gemini is already configured. Recommend using **LiteLLM** so you can benchmark GPT-4o vs. Claude 3.5 Sonnet vs. Gemini 1.5 Pro for Swedish exercise quality — then lock to the best. Which do you prefer to start with?

2. **Cloud target**: GCS + BigQuery. Native BigQuery tables with clustering by `chapter_id` / `paragraph_id`.

3. **Copyright / book sourcing**: EPUBs must be legally sourced. Will you be using Project Gutenberg (public domain Swedish classics), licensed textbooks, or your own content? This affects whether the pipeline needs a rights-metadata field.
