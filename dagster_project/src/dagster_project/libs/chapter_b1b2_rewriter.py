"""
Standalone LangGraph pipeline: rewrite a Swedish chapter at B1–B2 level.

The goal is a text that reads like the original book — with its natural
narrative voice, paragraph rhythm, and story structure intact — but
accessible to intermediate learners (CEFR B1–B2).  This is *not* a
summary or a third-person retelling; it is a full rewrite that stays
close to the source in length and feel.

Flow
----
[rewrite] ──► [judge] ──► END          (approved, or attempts ≥ 3)
      ▲               │
      └───────────────┘  (rejected with feedback)

Public API
----------
    graph = build_b1b2_rewriter_graph(llm)
    state = rewrite_chapter(graph, chapter_id, title, swedish_text)
"""

from __future__ import annotations

import operator
from typing import Annotated, Literal

from langchain_core.language_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import StateGraph
from typing_extensions import TypedDict

from langfuse import get_client
from langfuse.langchain import CallbackHandler

# Model used for rewriting
MODEL_NAME = "gemini-2.5-flash-preview-04-17"

# Initialize Langfuse client
langfuse = get_client()

# Initialize Langfuse CallbackHandler for Langchain (tracing)
langfuse_handler = CallbackHandler()

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


class B1B2RewriteState(TypedDict):
    chapter_id: str
    title: str
    swedish_text: str       # fluent Swedish input
    b1b2_text: str          # rewritten B1–B2 Swedish output
    judgment: str           # "approved" | "rejected"
    feedback: str           # latest rejection reason (empty if none)
    feedback_history: Annotated[
        list[str], operator.add
    ]                       # all rejection reasons across attempts
    attempts: int


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_REWRITE_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a skilled Swedish editor specialising in accessible language for "
                "intermediate (B1–B2 CEFR) readers. "
                "Your task is to rewrite the provided Swedish chapter so it is easy and "
                "enjoyable to read for B1–B2 learners, while keeping the spirit and "
                "structure of the original book.\n\n"
                "Guidelines:\n"
                "- Keep the same narrative voice and perspective as the original "
                "(first person if the original is first person, etc.).\n"
                "- Preserve the paragraph order and chapter structure — do not "
                "condense, summarise, or skip any part.\n"
                "- Use clear, correct Swedish sentences. Shorter sentences are "
                "preferred, but varied sentence length is fine.\n"
                "- Replace rare or advanced vocabulary with B1–B2 equivalents; "
                "keep domain-specific terms only when essential, and explain them "
                "briefly the first time they appear.\n"
                "- Retain all names, places, events, and dates from the original.\n"
                "- Do NOT paraphrase into a third-person summary or retelling. "
                "This must read as the chapter itself, not a description of it.\n"
                "- Do NOT add new information, commentary, or section headings.\n"
                "Return only the rewritten Swedish chapter text — no explanations, "
                "no metadata."
            ),
        ),
        (
            "human",
            (
                "Feedback from previous attempt (if any):\n{feedback_section}\n"
                "Chapter title: {title}\n\n"
                "Original Swedish text:\n{swedish_text}\n\n"
                "B1–B2 rewritten Swedish text:"
            ),
        ),
    ]
)

_JUDGE_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a quality-control judge for Swedish B1–B2 level rewrites. "
                "Evaluate whether the rewritten text meets ALL of the following criteria:\n"
                "1. Language is at B1–B2 CEFR level (intermediate, not too easy, not too hard).\n"
                "2. The rewrite follows the same structure and narrative voice as the original "
                "   — it reads as the chapter itself, not a summary or a third-person retelling.\n"
                "3. No content from the original has been omitted or condensed.\n"
                "4. Sentences are clear and pleasant to read.\n"
                'Reply with exactly "approved" if all criteria are met, '
                'or "rejected: <one-sentence reason>" if there are issues. '
                "No other output."
            ),
        ),
        (
            "human",
            (
                "Chapter title: {title}\n\n"
                "Original Swedish text:\n{swedish_text}\n\n"
                "B1–B2 rewritten text:\n{b1b2_text}\n\n"
                "Your verdict:"
            ),
        ),
    ]
)

# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------


def _make_rewrite_node(llm: BaseChatModel):
    chain = _REWRITE_PROMPT | llm

    def rewrite(state: B1B2RewriteState) -> dict:
        feedback_section = (
            f"Previous attempt was rejected. Feedback: {state['feedback']}\nPlease correct.\n\n"
            if state["feedback"]
            else ""
        )
        response = chain.invoke(
            {
                "title": state["title"],
                "swedish_text": state["swedish_text"],
                "feedback_section": feedback_section,
            },
            config={"callbacks": [langfuse_handler]},
        )
        c = response.content  # type: ignore
        text = (c if isinstance(c, str) else c[0]["text"]).strip()  # type: ignore
        return {
            "b1b2_text": text,
            "attempts": state["attempts"] + 1,
        }

    return rewrite


def _make_judge_node(llm: BaseChatModel):
    chain = _JUDGE_PROMPT | llm

    def judge(state: B1B2RewriteState) -> dict:
        response = chain.invoke(
            {
                "title": state["title"],
                "swedish_text": state["swedish_text"],
                "b1b2_text": state["b1b2_text"],
            },
            config={"callbacks": [langfuse_handler]},
        )
        c = response.content  # type: ignore
        verdict = (c if isinstance(c, str) else c[0]["text"]).strip().lower()  # type: ignore
        if verdict.startswith("approved"):
            return {"judgment": "approved"}
        feedback = verdict.removeprefix("rejected:").strip()
        return {
            "judgment": "rejected",
            "feedback": feedback,
            "feedback_history": [feedback],
        }

    return judge


# ---------------------------------------------------------------------------
# Conditional edge
# ---------------------------------------------------------------------------


def _should_retry(state: B1B2RewriteState) -> Literal["rewrite", "__end__"]:
    if state["judgment"] == "approved" or state["attempts"] >= 3:
        return "__end__"
    return "rewrite"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------


def build_b1b2_rewriter_graph(llm: BaseChatModel):
    """Return a compiled LangGraph that rewrites one Swedish chapter to B1–B2 level."""
    builder: StateGraph = StateGraph(B1B2RewriteState)

    builder.add_node("rewrite", _make_rewrite_node(llm))
    builder.add_node("judge", _make_judge_node(llm))

    builder.set_entry_point("rewrite")
    builder.add_edge("rewrite", "judge")
    builder.add_conditional_edges("judge", _should_retry)

    return builder.compile()


# ---------------------------------------------------------------------------
# Public runner
# ---------------------------------------------------------------------------


def rewrite_chapter(
    graph,
    chapter_id: str,
    title: str,
    swedish_text: str,
) -> B1B2RewriteState:
    """
    Run the B1–B2 rewrite graph for a single chapter.

    Returns
    -------
    B1B2RewriteState
        The final state; ``b1b2_text`` holds the B1–B2 rewritten Swedish text.
        Falls back to whatever the last attempt produced if all attempts are rejected.
    """
    initial_state: B1B2RewriteState = {
        "chapter_id": chapter_id,
        "title": title,
        "swedish_text": swedish_text,
        "b1b2_text": "",
        "judgment": "",
        "feedback": "",
        "feedback_history": [],
        "attempts": 0,
    }
    final_state: B1B2RewriteState = graph.invoke(initial_state)
    return final_state
