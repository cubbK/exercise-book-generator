"""
Bronze asset: RSS → raw storage.

Fetches RSS feeds from Swedish news sources and stores raw articles as JSON.
This layer is immutable — even malformed articles are kept.
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path

import feedparser
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Output,
    asset,
)

RSS_SOURCES = {
    "svt": "https://www.svt.se/nyheter/rss.xml",
    "dn": "https://www.dn.se/rss/",
    "aftonbladet": "https://rss.aftonbladet.se/rss2/small/pages/sections/senastenytt/",
}

daily_partitions = DailyPartitionsDefinition(start_date="2026-04-01", end_offset=1)


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Fetch raw RSS articles from Swedish news sources and store as JSON.",
)
def rss_raw_articles(context: AssetExecutionContext) -> Output:
    """
    Fetches RSS feeds for each configured source and writes raw items to
    data/bronze/{source}/{partition_date}.json.

    Returns a summary dict with counts per source.
    """
    partition_date = context.partition_key  # "YYYY-MM-DD"

    all_items: list[dict] = []
    items_per_source: dict[str, int] = {}

    for source_name, feed_url in RSS_SOURCES.items():
        context.log.info(f"Fetching feed: {source_name} ({feed_url})")
        feed = feedparser.parse(feed_url)

        raw_items = []
        for entry in feed.entries:
            raw_item = {
                "source": source_name,
                "partition_date": partition_date,
                "ingested_at": datetime.utcnow().isoformat(),
                "url": getattr(entry, "link", None),
                "title": getattr(entry, "title", None),
                "summary": getattr(entry, "summary", None),
                "published": getattr(entry, "published", None),
                # Store the raw content blob if present (may be HTML)
                "content_raw": (
                    entry.content[0].value
                    if hasattr(entry, "content") and entry.content
                    else None
                ),
                # Stable dedup key: SHA-256 of URL
                "url_hash": (
                    hashlib.sha256(entry.link.encode()).hexdigest()  # type: ignore
                    if hasattr(entry, "link") and entry.link
                    else None
                ),
            }
            raw_items.append(raw_item)

        output_path = Path("data") / "bronze" / source_name / f"{partition_date}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(raw_items, f, ensure_ascii=False, indent=2)

        context.log.info(
            f"Wrote {len(raw_items)} raw items for source '{source_name}' → {output_path}"
        )
        items_per_source[source_name] = len(raw_items)
        all_items.extend(raw_items)

    total = sum(items_per_source.values())
    context.log.info(f"Bronze ingestion complete. Total raw articles: {total}")

    return Output(
        value={"partition_date": partition_date, "items_per_source": items_per_source},
        metadata={
            "num_articles": total,
            "sources": list(RSS_SOURCES.keys()),
            "items_per_source": str(items_per_source),
        },
    )
