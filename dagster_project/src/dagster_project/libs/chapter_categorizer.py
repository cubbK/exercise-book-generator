"""
Standalone LangGraph pipeline: categorise a single book chapter.

Flow
----
[categorize] ──► [judge] ──► END          (approved, or attempts ≥ 3)
      ▲               │
      └───────────────┘  (rejected with feedback)

Public API
----------
    graph   = build_categorizer_graph(llm)
    cat, n  = categorize_chapter(graph, chapter_id, title, raw_text)
"""

from __future__ import annotations

from typing import Literal

from langchain_core.language_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

# ---------------------------------------------------------------------------
# Valid categories
# ---------------------------------------------------------------------------

VALID_CATEGORIES: frozenset[str] = frozenset(
    {
        "title_page",
        "dedication_page",
        "table_of_contents",
        "paragraph",
        "acknowledgments_page",
        "glossary",
        "bibliography",
        "other",
    }
)

_CATEGORIES_LIST = ", ".join(sorted(VALID_CATEGORIES))

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


class CategorizationState(TypedDict):
    chapter_id: str
    title: str
    raw_text_preview: str  # first 800 chars of raw_text
    category: str  # populated by categorize node
    judgment: str  # "approved" | "rejected"
    feedback: str  # rejection reason (empty string if none)
    attempts: int


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

_CATEGORIZE_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a strict classifier for language-exercise book chapters. "
                "Respond with exactly ONE lowercase word from this list: {categories}. "
                "No punctuation, no explanation."
            ),
        ),
        (
            "human",
            (
                "Chapter title: {title}\n\n"
                "Text preview:\n{raw_text_preview}\n\n"
                "{feedback_section}"
                "Category:"
            ),
        ),
    ]
)

_JUDGE_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a quality-control judge for chapter classification. "
                'Reply with exactly "approved" if the category is correct, '
                'or "rejected: <one-sentence reason>" if it is wrong. '
                "No other output."
            ),
        ),
        (
            "human",
            (
                "Chapter title: {title}\n\n"
                "Text preview:\n{raw_text_preview}\n\n"
                "Assigned category: {category}\n\n"
                "Valid categories: {categories}\n\n"
                "Your verdict:"
            ),
        ),
    ]
)


def _make_categorize_node(llm: BaseChatModel):
    chain = _CATEGORIZE_PROMPT | llm

    def categorize(state: CategorizationState) -> dict:
        feedback_section = (
            f"Previous attempt was rejected. Feedback: {state['feedback']}\nPlease correct.\n\n"
            if state["feedback"]
            else ""
        )
        response = chain.invoke(
            {
                "categories": _CATEGORIES_LIST,
                "title": state["title"],
                "raw_text_preview": state["raw_text_preview"],
                "feedback_section": feedback_section,
            }
        )
        c = response.content  # type: ignore
        raw = (c if isinstance(c, str) else c[0]["text"]).strip().lower()  # type: ignore
        category = raw if raw in VALID_CATEGORIES else "other"
        return {
            "category": category,
            "attempts": state["attempts"] + 1,
        }

    return categorize


def _make_judge_node(llm: BaseChatModel):
    chain = _JUDGE_PROMPT | llm

    def judge(state: CategorizationState) -> dict:
        response = chain.invoke(
            {
                "title": state["title"],
                "raw_text_preview": state["raw_text_preview"],
                "category": state["category"],
                "categories": _CATEGORIES_LIST,
            }
        )
        c = response.content  # type: ignore
        verdict = (c if isinstance(c, str) else c[0]["text"]).strip().lower()  # type: ignore
        if verdict.startswith("approved"):
            return {"judgment": "approved"}
        # "rejected: <reason>"
        feedback = verdict.removeprefix("rejected:").strip()
        return {"judgment": "rejected", "feedback": feedback}

    return judge


# ---------------------------------------------------------------------------
# Conditional edge
# ---------------------------------------------------------------------------


def _should_retry(state: CategorizationState) -> Literal["categorize", "__end__"]:
    if state["judgment"] == "approved" or state["attempts"] >= 3:
        return "__end__"
    return "categorize"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------


def build_categorizer_graph(llm: BaseChatModel):
    """Return a compiled LangGraph that categorises one chapter."""
    builder: StateGraph = StateGraph(CategorizationState)

    builder.add_node("categorize", _make_categorize_node(llm))
    builder.add_node("judge", _make_judge_node(llm))

    builder.set_entry_point("categorize")
    builder.add_edge("categorize", "judge")
    builder.add_conditional_edges("judge", _should_retry)

    return builder.compile()


# ---------------------------------------------------------------------------
# Public helper
# ---------------------------------------------------------------------------

_TEXT_PREVIEW_CHARS = 800


def categorize_chapter(
    graph,
    chapter_id: str,
    title: str,
    raw_text: str,
) -> CategorizationState:
    """
    Run the categorisation graph for a single chapter.

    Returns
    -------
    CategorizationState
        The final state of the categorization process.
    """
    initial_state: CategorizationState = {
        "chapter_id": chapter_id,
        "title": title,
        "raw_text_preview": raw_text[:_TEXT_PREVIEW_CHARS],
        "category": "",
        "judgment": "",
        "feedback": "",
        "attempts": 0,
    }
    final_state = graph.invoke(initial_state)
    return final_state
