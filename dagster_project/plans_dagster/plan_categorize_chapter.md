## Plan: Gold Layer Chapter Categorization Asset

**TL;DR**: New `chapter_categories` Dagster asset reads from `exercise_book_silver.chapters`, runs each chapter through a LangGraph categorize→judge loop (Vertex AI `gemini-2.0-flash`), writes `chapter_id + category` to `exercise_book_gold.chapter_categories`. Three new files, two modifications.

---

### Phase 1 — Dependencies

1. Add `langchain-google-vertexai>=2.0.0` to pyproject.toml deps list
2. `uv sync`

### Phase 2 — VertexAIResource

3. Create `src/dagster_project/resources/vertex_ai.py` — `ConfigurableResource` with fields `project`, `location`, `model_name="gemini-2.0-flash"` and a `get_llm() -> ChatVertexAI` method
4. Register in `definitions.py` as `vertex_ai=VertexAIResource(...)` — reads `GCP_PROJECT` + `VERTEX_AI_LOCATION` from env (can fall back to `BIGQUERY_LOCATION`)

### Phase 3 — LangGraph graph (standalone lib)

5. Create `src/dagster_project/lib/chapter_categorizer.py`:
   - `VALID_CATEGORIES` frozenset
   - `CategorizationState(TypedDict)`: `chapter_id`, `title`, `raw_text_preview`, `category`, `judgment`, `feedback`, `attempts`
   - `build_categorizer_graph(llm) → CompiledGraph` — the full LangGraph:
     ```
     [categorize] ──► [judge] ──► (approved OR attempts ≥ 3?) ──► END
           ▲                │ rejected (with feedback)
           └────────────────┘
     ```
   - `categorize` node: uses `ChatPromptTemplate`, lists valid categories, injects feedback from previous round, parses response → lowercase category, falls back to `"other"` if unrecognised
   - `judge` node: strict prompt — reply `"approved"` or `"rejected: <reason>"`, parses both cases
   - `should_retry` edge: `END` if approved or `attempts >= 3`
   - `categorize_chapter(graph, chapter_id, title, raw_text) -> tuple[str, int]` — public helper, sends first **800 chars** of raw_text

### Phase 4 — Gold Dagster asset

6. Create `src/dagster_project/defs/gold.py`:
   - `_CREATE_CHAPTER_CATEGORIES` DDL → `exercise_book_gold.chapter_categories`:
     - `chapter_id STRING NOT NULL, category STRING NOT NULL, attempts INT64 NOT NULL, categorized_at TIMESTAMP NOT NULL`
   - `_ensure_gold_tables()` — same DDL-if-not-exists pattern as silver.py
   - Anti-join SQL: fetch chapters in `silver.chapters` not yet in `gold.chapter_categories` (same idempotency pattern as silver.py)
   - `@asset(name="chapter_categories", group_name="gold", deps=["silver_chapters"])` with resources `storage: BigQueryStorage`, `vertex_ai: VertexAIResource`
   - Calls `build_categorizer_graph(vertex_ai.get_llm())` once, then loops per chapter → collects rows → `storage.write_df()`
   - `yield MaterializeResult(metadata={"chapters_categorized": N})`

---

**Relevant files**

| File                                             | Action                                 |
| ------------------------------------------------ | -------------------------------------- |
| pyproject.toml                                   | Add `langchain-google-vertexai>=2.0.0` |
| `src/dagster_project/resources/vertex_ai.py`     | Create — new resource                  |
| `src/dagster_project/lib/chapter_categorizer.py` | Create — LangGraph graph               |
| `src/dagster_project/defs/gold.py`               | Create — Dagster asset                 |
| definitions.py                                   | Register `VertexAIResource`            |

---

**Verification**

1. `uv sync` passes
2. `dagster asset list` shows `chapter_categories` in `gold` group
3. REPL test: `build_categorizer_graph` with a mock LLM terminates correctly
4. `dagster asset materialize --select chapter_categories` against dev BQ with 2-3 chapters
5. BQ: `SELECT category, COUNT(*) FROM exercise_book_gold.chapter_categories GROUP BY 1` — correct distribution
6. Re-materialize — row count unchanged (idempotency)

---

**Decisions**

- Graph lives in `lib/` so it's unit-testable without Dagster
- First 800 chars of `raw_text` only — saves tokens, still enough for classification
- Max 3 judge attempts — prevents infinite loops; last-attempt category is accepted regardless
- Auth: reuse ADC (same as BigQuery — no extra credentials needed for Vertex AI on GCP)
- `VERTEX_AI_LOCATION` env var (fallback to `BIGQUERY_LOCATION` works since both use `EU`)

---

Does this plan look good? Any adjustments — categories, table schema, batch size, or anything else — before handing off to implementation?
