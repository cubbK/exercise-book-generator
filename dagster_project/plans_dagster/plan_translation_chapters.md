## Plan: Gold Layer — Language, Translation & Summary (updated)

**TL;DR**: 4 separate assets → 4 separate tables → 4 separate LLM calls per chapter. Cleaner prompts, fully independent re-runs per enrichment type.

---

### Tables & dependency chain

```
silver_chapters
  └─ gold_chapter_language           → gold.chapter_language
       └─ gold_chapter_swedish_text  → gold.chapter_swedish_text (fluent)
            └─ gold_chapter_swedish_text_a2 → gold.chapter_swedish_text_a2
            └─ gold_chapter_summaries      → gold.chapter_summaries  ← reads fluent Swedish
```

**`gold.chapter_language`**

```
chapter_id, book_id, detected_language (ISO 639-1), attempts, detected_at
```

**`gold.chapter_swedish_text`**

```
chapter_id, book_id, source_language, swedish_text, was_translated (BOOL), attempts, translated_at
```

For already-Swedish chapters: `raw_text` copied directly, `was_translated=false`.

**`gold.chapter_swedish_text_a2`**

```
chapter_id, book_id, swedish_text_a2, attempts, translated_at
```

Input is `swedish_text` (fluent) — the simplification chain stays clean.

**`gold.chapter_summaries`**

```
chapter_id, book_id, summary, attempts, summarized_at
```

Input is `swedish_text` (fluent) — richer source material, output is A2-level Swedish.

---

### Steps

**Phase 1** — `libs/language_detector.py` + `gold_chapter_language` asset

**Phase 2** _(depends on Phase 1)_ — `libs/chapter_translator.py` + `gold_chapter_swedish_text` asset

**Phase 3** _(depends on Phase 2, two assets run in parallel)_

- `libs/chapter_translator_a2.py` + `gold_chapter_swedish_text_a2` — takes fluent Swedish as input
- `libs/chapter_summarizer.py` + `gold_chapter_summaries` — takes fluent Swedish as input

**Phase 4** — Register all 4 assets in `definitions.py`

---

### Relevant files

- gold.py — reference pattern (DDL + asset + DELETE/rewrite)
- chapter_categorizer.py — LangGraph loop pattern to replicate

---

### Decisions

- `gold_chapter_swedish_text_a2` and `gold_chapter_summaries` both depend on fluent Swedish (`chapter_swedish_text`) — they can be materialized in parallel in Dagster
- Summary is generated from fluent Swedish, not A2, for richer source material

Happy to proceed to implementation?
