"""
Asset checks — data quality validation for bronze and silver layers.

Checks run after each asset materialises and surface pass/fail results
in the Dagster UI without polluting the asset logic itself.

Bronze checks (bronze_epub_registry_raw):
    - Every row has a non-empty filename and storage_path
    - SHA-256 hash column is always 64 hex characters (valid hash)

Silver checks (silver_books):
    - title is never null or blank
    - No duplicate file_id (one row per EPUB)

Silver checks (silver_chapters):
    - title is never null or blank
    - raw_text is never null or suspiciously short (< 50 chars)
    - chapter_order is unique per book (no duplicates)
    - Every book has at least one chapter
"""

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from dagster_project.resources.storage import BigQueryStorage

_BRONZE = "exercise_book_bronze"
_SILVER = "exercise_book_silver"

# ---------------------------------------------------------------------------
# Bronze — bronze_epub_registry_raw
# ---------------------------------------------------------------------------


@asset_check(
    asset="bronze_epub_registry_raw",
    description="Every staged EPUB must have a non-empty filename and storage_path.",
)
def epub_registry_no_blank_paths(storage: BigQueryStorage) -> AssetCheckResult:
    rows = storage.execute(
        f"SELECT COUNT(*) AS cnt "
        f"FROM `{storage.project}.{_BRONZE}.epub_registry_raw` "
        f"WHERE TRIM(filename) = '' OR filename IS NULL "
        f"   OR TRIM(storage_path) = '' OR storage_path IS NULL"
    )
    count = int(rows[0]["cnt"])
    return AssetCheckResult(
        passed=count == 0,
        metadata={"rows_with_blank_paths": count},
    )


@asset_check(
    asset="bronze_epub_registry_raw",
    description="sha256_hash must always be a 64-character hex string.",
)
def epub_registry_valid_sha256(storage: BigQueryStorage) -> AssetCheckResult:
    rows = storage.execute(
        f"SELECT COUNT(*) AS cnt "
        f"FROM `{storage.project}.{_BRONZE}.epub_registry_raw` "
        f"WHERE NOT REGEXP_CONTAINS(sha256_hash, r'^[0-9a-f]{{64}}$')"
    )
    count = int(rows[0]["cnt"])
    return AssetCheckResult(
        passed=count == 0,
        metadata={"invalid_hash_count": count},
    )


# ---------------------------------------------------------------------------
# Silver — silver_books
# ---------------------------------------------------------------------------


@asset_check(
    asset="silver_books",
    description="Every book must have a non-empty title.",
)
def silver_books_have_title(storage: BigQueryStorage) -> AssetCheckResult:
    rows = storage.execute(
        f"SELECT COUNT(*) AS cnt "
        f"FROM `{storage.project}.{_SILVER}.books` "
        f"WHERE title IS NULL OR TRIM(title) = ''"
    )
    count = int(rows[0]["cnt"])
    return AssetCheckResult(
        passed=count == 0,
        metadata={"missing_title_count": count},
    )


@asset_check(
    asset="silver_books",
    description="file_id must be unique in silver.books — one row per EPUB.",
)
def silver_books_no_duplicate_file_id(storage: BigQueryStorage) -> AssetCheckResult:
    rows = storage.execute(
        f"SELECT COUNT(*) AS cnt FROM ("
        f"  SELECT file_id "
        f"  FROM `{storage.project}.{_SILVER}.books` "
        f"  GROUP BY file_id HAVING COUNT(*) > 1"
        f")"
    )
    count = int(rows[0]["cnt"])
    return AssetCheckResult(
        passed=count == 0,
        metadata={"duplicate_file_id_count": count},
    )


# ---------------------------------------------------------------------------
# Silver — silver_chapters
# ---------------------------------------------------------------------------


@asset_check(
    asset="silver_chapters",
    description="Every chapter must have a non-empty title.",
)
def silver_chapters_have_title(storage: BigQueryStorage) -> AssetCheckResult:
    rows = storage.execute(
        f"SELECT COUNT(*) AS cnt "
        f"FROM `{storage.project}.{_SILVER}.chapters` "
        f"WHERE title IS NULL OR TRIM(title) = ''"
    )
    count = int(rows[0]["cnt"])
    return AssetCheckResult(
        passed=count == 0,
        metadata={"empty_title_count": count},
    )


@asset_check(
    asset="silver_chapters",
    description=(
        "raw_text must not be null or suspiciously short (< 50 chars). "
        "Warn rather than fail — some front-matter chapters are intentionally brief."
    ),
)
def silver_chapters_have_text(storage: BigQueryStorage) -> AssetCheckResult:
    rows = storage.execute(
        f"SELECT COUNT(*) AS cnt "
        f"FROM `{storage.project}.{_SILVER}.chapters` "
        f"WHERE raw_text IS NULL OR LENGTH(TRIM(raw_text)) < 50"
    )
    count = int(rows[0]["cnt"])
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"short_or_empty_text_count": count},
    )


@asset_check(
    asset="silver_chapters",
    description="chapter_order must be unique within each book.",
)
def silver_chapters_order_unique(storage: BigQueryStorage) -> AssetCheckResult:
    rows = storage.execute(
        f"SELECT COUNT(*) AS cnt FROM ("
        f"  SELECT book_id, chapter_order "
        f"  FROM `{storage.project}.{_SILVER}.chapters` "
        f"  GROUP BY book_id, chapter_order HAVING COUNT(*) > 1"
        f")"
    )
    count = int(rows[0]["cnt"])
    return AssetCheckResult(
        passed=count == 0,
        metadata={"duplicate_order_count": count},
    )


@asset_check(
    asset="silver_chapters",
    description="Every book in silver.books must have at least one chapter.",
)
def silver_books_have_chapters(storage: BigQueryStorage) -> AssetCheckResult:
    rows = storage.execute(
        f"SELECT COUNT(*) AS cnt "
        f"FROM `{storage.project}.{_SILVER}.books` AS b "
        f"LEFT JOIN `{storage.project}.{_SILVER}.chapters` AS c USING (book_id) "
        f"WHERE c.book_id IS NULL"
    )
    count = int(rows[0]["cnt"])
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"books_without_chapters": count},
    )
