"""
EPUB parsing library — extracts structured book/chapter data.

Flow:
    raw bytes → ParsedBook (metadata + chapters)

Strategy:
    1. Extract the TOC (publisher-defined chapter list).
    2. For each TOC entry, fetch the HTML file, inject sentinel markers,
       convert to plain text with html2text, and slice between markers.
    3. Fallback to all HTML documents (spine order) when the TOC is absent.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from urllib.parse import unquote

import html2text
from bs4 import BeautifulSoup
from bs4.element import NavigableString
from ebooklib import epub, ITEM_DOCUMENT


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ParsedChapter:
    order: int
    title: str
    raw_text: str


@dataclass
class ParsedBook:
    title: str
    author: str
    language: str
    isbn: str | None
    publication_year: int | None
    chapters: list[ParsedChapter]


# ---------------------------------------------------------------------------
# HTML → plain text
# ---------------------------------------------------------------------------

_NOISE_TAGS = ["script", "style", "nav", "header", "footer", "aside"]
_START = "!!EPUB_START!!"
_STOP = "!!EPUB_STOP!!"

_h = html2text.HTML2Text()
_h.ignore_links = True
_h.ignore_images = True
_h.body_width = 0
_h.unicode_snob = True


def _to_text(
    html_bytes: bytes,
    start_id: str | None = None,
    stop_id: str | None = None,
) -> str:
    """Convert HTML bytes to plain text, optionally sliced by anchor ids."""
    soup = BeautifulSoup(html_bytes, "lxml")
    for tag in soup.find_all(_NOISE_TAGS):
        tag.decompose()

    if start_id:
        el = soup.find(id=start_id) or soup.find(attrs={"name": start_id})
        if el:
            el.insert_before(NavigableString(_START))
    if stop_id:
        el = soup.find(id=stop_id) or soup.find(attrs={"name": stop_id})
        if el:
            el.insert_before(NavigableString(_STOP))

    text = _h.handle(str(soup.body or soup))

    if start_id and _START in text:
        text = text.split(_START, 1)[1]
    if _STOP in text:
        text = text.split(_STOP, 1)[0]

    return text.strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _extract_metadata(book: epub.EpubBook) -> dict:
    def first(items):
        return items[0][0] if items else None

    identifier = first(book.get_metadata("DC", "identifier"))
    date_raw = first(book.get_metadata("DC", "date"))

    isbn: str | None = None
    if identifier:
        candidate = identifier.split(":")[-1].strip().replace("-", "").replace(" ", "")
        if (
            len(candidate) == 13
            and candidate.isdigit()
            and candidate[:3] in ("978", "979")
        ):
            isbn = candidate

    year: int | None = None
    if date_raw:
        y = date_raw.strip()[:4]
        if len(y) == 4 and y.isdigit():
            year = int(y)

    return {
        "title": first(book.get_metadata("DC", "title")) or "Unknown",
        "author": first(book.get_metadata("DC", "creator")) or "Unknown",
        "language": first(book.get_metadata("DC", "language")) or "sv",
        "isbn": isbn,
        "publication_year": year,
    }


def _flatten_toc(toc) -> list[tuple[str, str]]:
    """Recursively flatten ebooklib TOC into (title, href) pairs."""
    result = []
    for item in toc:
        if isinstance(item, epub.Link):
            result.append((item.title or "", item.href or ""))
        elif isinstance(item, tuple) and len(item) == 2:
            section, children = item
            if href := getattr(section, "href", None):
                result.append((getattr(section, "title", "") or "", href))
            result.extend(_flatten_toc(children))
    return result


def _top_level_toc(toc) -> list[tuple[str, str]]:
    """Return only top-level TOC entries as (title, href) pairs, ignoring sub-entries."""
    result = []
    for item in toc:
        if isinstance(item, epub.Link):
            result.append((item.title or "", item.href or ""))
        elif isinstance(item, tuple) and len(item) == 2:
            section, _children = item
            if href := getattr(section, "href", None):
                result.append((getattr(section, "title", "") or "", href))
    return result


def parse_epub(data: bytes) -> ParsedBook:
    """Parse EPUB bytes into a ParsedBook with ordered chapters."""
    book = epub.read_epub(BytesIO(data))
    meta = _extract_metadata(book)

    # file_name → item
    file_map = {
        unquote(item.file_name).lstrip("/"): item
        for item in book.get_items_of_type(ITEM_DOCUMENT)
    }

    def resolve(file_part: str):
        return file_map.get(file_part) or next(
            (
                v
                for k, v in file_map.items()
                if k.split("/")[-1] == file_part.split("/")[-1]
            ),
            None,
        )

    # --- TOC-based extraction -------------------------------------------
    # Use only top-level TOC entries so that ## chapters include all ###
    # sub-entries rather than being split into separate chapters.
    entries = [
        (title.strip(), *((unquote(href).split("#", 1) + [None])[:2]))
        for title, href in _top_level_toc(book.toc)
    ]
    # Normalise: entries[i] = (title, file_part, anchor_or_None)
    entries = [(t, (f or "").lstrip("/"), a or None) for t, f, a in entries]

    chapters: list[ParsedChapter] = []
    seen: set[tuple[str, str | None]] = set()

    for i, (title, file_part, anchor) in enumerate(entries):
        key = (file_part, anchor)
        if key in seen:
            continue
        seen.add(key)

        item = resolve(file_part)
        if not item:
            continue

        # Stop at the next TOC anchor that lives in the same file
        stop = next((a for _, f, a in entries[i + 1 :] if f == file_part), None)

        text = _to_text(item.get_content(), anchor, stop)
        if text:
            chapters.append(
                ParsedChapter(
                    order=len(chapters),
                    title=title or f"Chapter {len(chapters) + 1}",
                    raw_text=text,
                )
            )

    # --- Fallback: all documents sorted by spine order ------------------
    if len(chapters) < 2:
        spine_pos = {item_id: i for i, (item_id, _) in enumerate(book.spine)}
        docs = sorted(
            book.get_items_of_type(ITEM_DOCUMENT),
            key=lambda d: (spine_pos.get(d.id, 9999), d.file_name),
        )
        chapters = []
        for item in docs:
            text = _to_text(item.get_content())
            if text:
                order = len(chapters)
                base = item.file_name.split("/")[-1].rsplit(".", 1)[0]
                chapters.append(
                    ParsedChapter(
                        order=order,
                        title=base or f"Chapter {order + 1}",
                        raw_text=text,
                    )
                )

    return ParsedBook(**meta, chapters=chapters)
