"""
Integration tests for epub_parser using real EPUB files.

Put any .epub file under dagster_project/data/ and it will be picked up
automatically.  The tests are skipped (not failed) when no books are present.

Usage:
    uv run pytest tests/test_epub_parser.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from dagster_project.libs.epub_parser import (
    MERGE_THRESHOLD,
    ParsedBook,
    ParsedChapter,
    TocNode,
    _emit_chapters,
    _hierarchy_key,
    parse_epub,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent.parent / "data"

_epub_files: list[Path] = sorted(DATA_DIR.glob("**/*.epub"))


def _epub_ids():
    return [p.name for p in _epub_files] or ["<no epub files found>"]


@pytest.fixture(params=_epub_files, ids=_epub_ids())
def parsed_book(request: pytest.FixtureRequest) -> ParsedBook:
    epub_path: Path = request.param
    return parse_epub(epub_path.read_bytes())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _total_text_length(book: ParsedBook) -> int:
    return sum(len(ch.raw_text) for ch in book.chapters)


# ---------------------------------------------------------------------------
# Tests — skipped when data/ contains no EPUBs
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _epub_files, reason="No .epub files found in data/")
class TestParsedBook:
    def test_returns_parsed_book_instance(self, parsed_book):
        assert isinstance(parsed_book, ParsedBook)

    def test_title_is_non_empty_string(self, parsed_book):
        assert isinstance(parsed_book.title, str)
        assert parsed_book.title.strip(), "title should not be blank"

    def test_author_is_non_empty_string(self, parsed_book):
        assert isinstance(parsed_book.author, str)
        assert parsed_book.author.strip(), "author should not be blank"

    def test_language_is_two_or_three_letter_code(self, parsed_book):
        assert re.match(r"^[a-z]{2,3}$", parsed_book.language), (
            f"unexpected language value: {parsed_book.language!r}"
        )

    def test_isbn_is_none_or_13_digits(self, parsed_book):
        if parsed_book.isbn is not None:
            assert re.match(r"^\d{13}$", parsed_book.isbn), (
                f"ISBN not 13 digits: {parsed_book.isbn!r}"
            )

    def test_publication_year_is_none_or_plausible(self, parsed_book):
        if parsed_book.publication_year is not None:
            assert 1900 <= parsed_book.publication_year <= 2100, (
                f"publication_year out of range: {parsed_book.publication_year}"
            )


@pytest.mark.skipif(not _epub_files, reason="No .epub files found in data/")
class TestParsedChapters:
    def test_at_least_one_chapter(self, parsed_book):
        assert len(parsed_book.chapters) >= 1

    def test_chapters_are_parsed_chapter_instances(self, parsed_book):
        for ch in parsed_book.chapters:
            assert isinstance(ch, ParsedChapter)

    def test_chapter_order_is_zero_based_sequential(self, parsed_book):
        orders = [ch.order for ch in parsed_book.chapters]
        assert orders == list(range(len(orders))), (
            f"chapter orders are not sequential: {orders}"
        )

    def test_each_chapter_has_non_empty_title(self, parsed_book):
        for ch in parsed_book.chapters:
            assert ch.title.strip(), f"chapter {ch.order} has a blank title"

    def test_each_chapter_has_non_empty_text(self, parsed_book):
        for ch in parsed_book.chapters:
            assert ch.raw_text.strip(), f"chapter {ch.order} ({ch.title!r}) has no text"

    def test_total_text_is_substantial(self, parsed_book):
        """Sanity check: a real book should have > 1 000 characters of text."""
        assert _total_text_length(parsed_book) > 1_000, (
            f"total extracted text is suspiciously short: {_total_text_length(parsed_book)} chars"
        )

    def test_no_sentinel_markers_leaked_into_text(self, parsed_book):
        """Internal sentinel strings must never appear in the output."""
        for ch in parsed_book.chapters:
            assert "!!EPUB_START!!" not in ch.raw_text
            assert "!!EPUB_STOP!!" not in ch.raw_text

    def test_no_duplicate_chapter_orders(self, parsed_book):
        orders = [ch.order for ch in parsed_book.chapters]
        assert len(orders) == len(set(orders)), "duplicate chapter orders found"

    def test_chapters_are_returned_sorted_by_order(self, parsed_book):
        orders = [ch.order for ch in parsed_book.chapters]
        assert orders == sorted(orders)

    def test_each_chapter_has_reasonable_length(self, parsed_book):
        """Each chapter should be at most 200 000 characters (no merged-giant chapters)."""
        MAX_CHARS = 200_000
        for ch in parsed_book.chapters:
            length = len(ch.raw_text)
            assert length <= MAX_CHARS, (
                f"chapter {ch.order} ({ch.title!r}) is suspiciously long: {length} chars"
            )


# ---------------------------------------------------------------------------
# Unit tests — hierarchy key generation (no EPUB files required)
# ---------------------------------------------------------------------------


class TestHierarchyKey:
    def test_empty_path_returns_chapter(self):
        assert _hierarchy_key([]) == "chapter"

    def test_single_level(self):
        assert _hierarchy_key([1]) == "chapter_level1"
        assert _hierarchy_key([2]) == "chapter_level2"
        assert _hierarchy_key([5]) == "chapter_level5"

    def test_two_levels(self):
        assert _hierarchy_key([1, 1]) == "chapter_level1_sublevel1"
        assert _hierarchy_key([3, 2]) == "chapter_level3_sublevel2"

    def test_three_levels(self):
        assert _hierarchy_key([1, 2, 3]) == "chapter_level1_sublevel2_sublevel3"


def _make_node(text: str, children=None) -> TocNode:
    return TocNode(
        title="T",
        file_part="f.html",
        anchor=None,
        depth=0,
        children=children or [],
        text=text,
    )


class TestEmitChapters:
    def test_leaf_node_emits_single_chapter(self):
        node = _make_node("Hello world content")
        chapters: list[ParsedChapter] = []
        _emit_chapters(node, [1], chapters)
        assert len(chapters) == 1
        assert chapters[0].chapter_key == "chapter_level1"
        assert chapters[0].raw_text == "Hello world content"

    def test_empty_leaf_emits_nothing(self):
        node = _make_node("")
        chapters: list[ParsedChapter] = []
        _emit_chapters(node, [1], chapters)
        assert chapters == []

    def test_first_content_root_gets_level1(self):
        """Roots with text start at level1, not level2."""
        roots = [_make_node(""), _make_node("Real content here")]
        chapters: list[ParsedChapter] = []
        key_idx = 0
        from dagster_project.libs.epub_parser import _collect_text

        for root in roots:
            if not _collect_text(root):
                continue
            key_idx += 1
            _emit_chapters(root, [key_idx], chapters)
        assert len(chapters) == 1
        assert chapters[0].chapter_key == "chapter_level1"

    def test_multiple_roots_sequential_keys(self):
        roots = [_make_node("Chapter one text"), _make_node("Chapter two text")]
        chapters: list[ParsedChapter] = []
        from dagster_project.libs.epub_parser import _collect_text

        key_idx = 0
        for root in roots:
            if not _collect_text(root):
                continue
            key_idx += 1
            _emit_chapters(root, [key_idx], chapters)
        keys = [ch.chapter_key for ch in chapters]
        assert keys == ["chapter_level1", "chapter_level2"]

    def test_small_parent_with_children_is_merged(self):
        child = _make_node("Child text")
        parent = _make_node("Parent text", children=[child])
        chapters: list[ParsedChapter] = []
        _emit_chapters(parent, [1], chapters)
        # small total → merged into one chapter
        assert len(chapters) == 1
        assert chapters[0].chapter_key == "chapter_level1"
        assert "Parent text" in chapters[0].raw_text
        assert "Child text" in chapters[0].raw_text

    def test_large_parent_with_children_is_not_merged(self):
        big_text = "x" * (MERGE_THRESHOLD + 1)
        child = _make_node("Child text")
        parent = _make_node(big_text, children=[child])
        chapters: list[ParsedChapter] = []
        _emit_chapters(parent, [1], chapters)
        # large total → parent and child emitted separately
        assert len(chapters) == 2
        assert chapters[0].chapter_key == "chapter_level1"
        assert chapters[1].chapter_key == "chapter_level1_sublevel1"

    def test_sublevel_keys_assigned_correctly(self):
        child1 = _make_node("Section A")
        child2 = _make_node("Section B")
        big_text = "x" * (MERGE_THRESHOLD + 1)
        parent = _make_node(big_text, children=[child1, child2])
        chapters: list[ParsedChapter] = []
        _emit_chapters(parent, [3], chapters)
        keys = [ch.chapter_key for ch in chapters]
        assert keys == [
            "chapter_level3",
            "chapter_level3_sublevel1",
            "chapter_level3_sublevel2",
        ]

    def test_depth2_node_never_merged_regardless_of_size(self):
        # depth 3: leaf
        great_grandchild = _make_node("Great-grandchild content")
        # depth 2: has a child; big text forces depth-1 parent not to merge
        big_text2 = "y" * (MERGE_THRESHOLD + 1)
        grandchild = _make_node(big_text2, children=[great_grandchild])
        # depth 1: total text is huge (grandchild alone exceeds threshold) → no merge
        child = _make_node("Child text", children=[grandchild])
        # depth 0: also huge
        big_text = "x" * (MERGE_THRESHOLD + 1)
        parent = _make_node(big_text, children=[child])
        chapters: list[ParsedChapter] = []
        _emit_chapters(parent, [1], chapters)
        keys = [ch.chapter_key for ch in chapters]
        # depth-2 node must be emitted separately (never merged)
        assert "chapter_level1_sublevel1_sublevel1" in keys
        # its leaf child must also be emitted separately
        assert "chapter_level1_sublevel1_sublevel1_sublevel1" in keys


# ---------------------------------------------------------------------------
# Integration — chapter_key format on real EPUBs
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _epub_files, reason="No .epub files found in data/")
class TestChapterKeysIntegration:
    _VALID_KEY = re.compile(r"^chapter(_level\d+(_sublevel\d+)*)?$")

    def test_all_chapter_keys_match_expected_format(self, parsed_book):
        for ch in parsed_book.chapters:
            assert self._VALID_KEY.match(ch.chapter_key), (
                f"chapter {ch.order} ({ch.title!r}) has unexpected key: {ch.chapter_key!r}"
            )

    def test_first_chapter_key_starts_at_level1(self, parsed_book):
        first_key = parsed_book.chapters[0].chapter_key
        assert first_key.startswith("chapter_level1"), (
            f"first chapter key should start at level1, got {first_key!r}"
        )

    def test_chapter_keys_are_unique(self, parsed_book):
        keys = [ch.chapter_key for ch in parsed_book.chapters]
        assert len(keys) == len(set(keys)), (
            f"duplicate chapter_keys found: {[k for k in keys if keys.count(k) > 1]}"
        )
