Here's the best practice for this migration:

## The Pattern: Dagster writes staging → dbt deduplicates/materializes

### What happens to `_CREATE_TABLE` and `_EXISTING_HASHES`

| Current (inline SQL)                   | dbt equivalent                                          |
| -------------------------------------- | ------------------------------------------------------- |
| `_CREATE_TABLE`                        | Removed — dbt owns DDL via `materialized='incremental'` |
| `_EXISTING_HASHES` + manual dedup loop | Removed — dbt's `unique_key` handles it natively        |

---

### Step 1 — Dagster asset writes to a raw/staging table (no dedup)

The Python asset keeps the GCS logic (only dbt can't compute `sha256` of binary blobs) but writes **all rows** to a staging table without checking for duplicates:

```python
# defs/bronze.py
@asset(group_name="bronze")
def epub_registry_raw(storage: BigQueryStorage, gcs: GCSObjectStoreResource) -> None:
    epub_blobs = gcs.list_epubs()
    rows = []
    for blob in epub_blobs:
        data = gcs.download_bytes(blob.name)
        rows.append({
            "file_id": str(uuid.uuid4()),
            "filename": blob.name.split("/")[-1],
            "storage_path": f"gs://{gcs.bucket}/{blob.name}",
            "sha256_hash": hashlib.sha256(data).hexdigest(),
            "uploaded_at": datetime.now(timezone.utc),
            "status": "registered",
        })
    if rows:
        storage.write_df("epub_registry_raw", pd.DataFrame(rows))
```

No `_CREATE_TABLE`, no `_EXISTING_HASHES` — that logic moves entirely to dbt.

---

### Step 2 — dbt incremental model replaces both SQL constants

```sql
-- models/bronze/epub_registry.sql
{{ config(
    materialized='incremental',
    unique_key='sha256_hash',
    on_schema_change='sync_all_columns'
) }}

SELECT
    file_id,
    filename,
    storage_path,
    sha256_hash,
    uploaded_at,
    status
FROM {{ source('bronze_raw', 'epub_registry_raw') }}

{% if is_incremental() %}
  WHERE sha256_hash NOT IN (SELECT sha256_hash FROM {{ this }})
{% endif %}
```

- `materialized='incremental'` replaces `_CREATE_TABLE` (dbt runs `CREATE TABLE IF NOT EXISTS` on first run, `MERGE`/`INSERT` on subsequent ones)
- `unique_key='sha256_hash'` + the `{% if is_incremental() %}` filter replaces `_EXISTING_HASHES` + the manual set-membership check

---

### Step 3 — Wire dbt into Dagster

```python
# defs/dbt.py
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

dbt_project = DbtProject(project_dir=Path(__file__).parent.parent.parent / "dbt")

@dbt_assets(manifest=dbt_project.manifest_path)
def my_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
```

Dagster will automatically infer the dependency: `epub_registry_raw` (Python asset) → `epub_registry` (dbt model), giving you lineage for free.

---

### Add dependencies

```
uv add dagster-dbt dbt-bigquery
```

---

### Summary of what disappears from Python

- `_CREATE_TABLE` — gone entirely
- `_EXISTING_HASHES` — gone entirely
- `existing_hashes: set[str]` fetch + `if sha256 in existing_hashes` check — gone
- `bq.query(_CREATE_TABLE...).result()` block — gone

The same pattern applies to `_CREATE_BOOKS` / `_CREATE_CHAPTERS` in silver.py when you migrate that layer.
