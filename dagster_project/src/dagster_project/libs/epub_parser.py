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

from dataclasses import dataclass, field
from io import BytesIO
from urllib.parse import unquote

import warnings

import html2text
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from bs4.element import NavigableString
from ebooklib import epub, ITEM_DOCUMENT

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ParsedChapter:
    order: int
    title: str
    chapter_key: str
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


# ---------------------------------------------------------------------------
# TOC tree
# ---------------------------------------------------------------------------

MERGE_THRESHOLD = 30_000
_MIN_CHAPTER_CHARS = 100  # nodes with less own-text are section dividers; skip them


@dataclass
class TocNode:
    title: str
    file_part: str
    anchor: str | None
    depth: int
    children: list["TocNode"] = field(default_factory=list)
    text: str = ""


def _build_toc_tree(toc, depth: int = 0) -> list[TocNode]:
    """Recursively build TocNode tree from ebooklib TOC, preserving hierarchy."""
    result: list[TocNode] = []
    for item in toc:
        if isinstance(item, epub.Link):
            href = item.href or ""
            file_part, _, anchor = href.partition("#")
            result.append(
                TocNode(
                    title=item.title or "",
                    file_part=unquote(file_part).lstrip("/"),
                    anchor=anchor or None,
                    depth=depth,
                )
            )
        elif isinstance(item, tuple) and len(item) == 2:
            section, children = item
            href = getattr(section, "href", "") or ""
            file_part, _, anchor = href.partition("#")
            node = TocNode(
                title=getattr(section, "title", "") or "",
                file_part=unquote(file_part).lstrip("/"),
                anchor=anchor or None,
                depth=depth,
                children=_build_toc_tree(children, depth + 1),
            )
            result.append(node)
    return result


def _flatten_nodes(nodes: list[TocNode]) -> list[TocNode]:
    """Pre-order flatten of TocNode tree."""
    result: list[TocNode] = []
    for node in nodes:
        result.append(node)
        result.extend(_flatten_nodes(node.children))
    return result


def _hierarchy_key(path: list[int]) -> str:
    """Convert a 1-based index path to a hierarchy key string.

    Examples::
        [1]       → 'chapter_level1'
        [2]       → 'chapter_level2'
        [1, 2]    → 'chapter_level1_sublevel2'
        [1, 2, 3] → 'chapter_level1_sublevel2_sublevel3'
    """
    if not path:
        return "chapter"
    parts = [f"level{path[0]}"] + [f"sublevel{i}" for i in path[1:]]
    return "chapter_" + "_".join(parts)


def _collect_text(node: TocNode) -> str:
    """Concatenate a node's text with all descendants' text."""
    parts = [node.text] + [_collect_text(c) for c in node.children]
    return "\n\n".join(p for p in parts if p)


def _emit_chapters(
    node: TocNode,
    path: list[int],
    chapters: list[ParsedChapter],
) -> None:
    """Recursively emit ParsedChapter entries with merge logic.

    Merge rule:
    - Depth < 2 (levels 1–2) with total text < MERGE_THRESHOLD: unite node +
      all descendants into a single chapter.
    - Depth >= 2 (level 3+) or total text >= MERGE_THRESHOLD: emit the node
      as its own chapter and recurse into children.
    """
    total_text = _collect_text(node)
    depth = len(path) - 1  # 0-based

    merge = bool(node.children) and depth < 2 and len(total_text) < MERGE_THRESHOLD

    key = _hierarchy_key(path)
    if merge:
        if total_text:
            chapters.append(
                ParsedChapter(
                    order=len(chapters),
                    title=node.title or key,
                    chapter_key=key,
                    raw_text=total_text,
                )
            )
    else:
        if node.text:
            chapters.append(
                ParsedChapter(
                    order=len(chapters),
                    title=node.title or key,
                    chapter_key=key,
                    raw_text=node.text,
                )
            )
        for i, child in enumerate(node.children, 1):
            _emit_chapters(child, path + [i], chapters)


def parse_epub(data: bytes) -> ParsedBook:
    """Parse EPUB bytes into a ParsedBook with hierarchical chapters.

    TOC levels are preserved.  When a node + all its descendants fit within
    MERGE_THRESHOLD characters they are merged into a single chapter; otherwise
    each level is emitted as its own chapter.  Nodes at depth >= 2 (level 3+)
    are always kept separate regardless of size.
    """
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

    # --- Build TOC tree and extract text per node -----------------------
    roots = _build_toc_tree(book.toc)
    flat_nodes = _flatten_nodes(roots)

    seen: set[tuple[str, str | None]] = set()
    for i, node in enumerate(flat_nodes):
        file_key = (node.file_part, node.anchor)
        if file_key in seen:
            continue
        seen.add(file_key)

        item = resolve(node.file_part)
        if not item:
            continue

        # Stop at the anchor of the next node that shares the same file
        stop = next(
            (n.anchor for n in flat_nodes[i + 1 :] if n.file_part == node.file_part),
            None,
        )
        node.text = _to_text(item.get_content(), node.anchor, stop)

    # --- Emit chapters with merge logic ---------------------------------
    chapters: list[ParsedChapter] = []
    key_idx = 0
    for root in roots:
        if not _collect_text(root):
            continue
        key_idx += 1
        _emit_chapters(root, [key_idx], chapters)

    # --- Fallback: all documents sorted by spine order ------------------
    if len(chapters) < 2:
        spine_pos = {item_id: idx for idx, (item_id, _) in enumerate(book.spine)}
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
                        chapter_key=f"chapter_level{order + 1}",
                        raw_text=text,
                    )
                )

    return ParsedBook(**meta, chapters=chapters)
