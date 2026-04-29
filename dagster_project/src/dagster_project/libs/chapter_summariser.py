"""
Standalone LangGraph pipeline: summarise a Swedish chapter at A2 level.

Flow
----
[summarise] ──► [judge] ──► END          (approved, or attempts ≥ 3)
       ▲               │
       └───────────────┘  (rejected with feedback)

Public API
----------
    graph  = build_summariser_graph(llm)
    state  = summarise_chapter(graph, chapter_id, title, a2_text)
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

# Model used for summarisation
MODEL_NAME = "gemini-2.5-flash-preview-04-17"

# Initialize Langfuse client
langfuse = get_client()

# Initialize Langfuse CallbackHandler for Langchain (tracing)
langfuse_handler = CallbackHandler()

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


class SummarisationState(TypedDict):
    chapter_id: str
    title: str
    a2_text: str  # A2-level Swedish input
    summary: str  # short A2-level summary output
    judgment: str  # "approved" | "rejected"
    feedback: str  # latest rejection reason (empty if none)
    feedback_history: Annotated[
        list[str], operator.add
    ]  # all rejection reasons across attempts
    attempts: int


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_SUMMARISE_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are an expert at writing summaries for Swedish language learners at A2 level. "
                "Write a short summary (5-7 sentences) of the provided Swedish text. "
                "Guidelines:\n"
                "- Use only common, everyday Swedish vocabulary (A1–A2 CEFR level).\n"
                "- Use short, simple sentences (max ~15 words each).\n"
                "- Capture only the most important points from the chapter.\n"
                "- Do NOT add information that is not in the original text.\n"
                "Return only the summary text — no headings, no explanations, no metadata."
            ),
        ),
        (
            "human",
            (
                "Feedback from previous attempt (if any):\n{feedback_section}\n"
                "Chapter title: {title}\n\n"
                "Chapter text:\n{a2_text}\n\n"
                "A2-level summary:"
            ),
        ),
    ]
)

_JUDGE_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a quality-control judge for Swedish A2-level chapter summaries. "
                "Evaluate whether the summary meets these criteria:\n"
                "- It is 5–7 sentences long.\n"
                "- It uses only A1–A2 CEFR vocabulary.\n"
                "- Sentences are short and simple.\n"
                "- It covers the key points of the chapter without adding new information.\n"
                'Reply with exactly "approved" if the summary meets all criteria, '
                'or "rejected: <one-sentence reason>" if there are issues. '
                "No other output."
            ),
        ),
        (
            "human",
            (
                "Chapter title: {title}\n\n"
                "Chapter text:\n{a2_text}\n\n"
                "Summary:\n{summary}\n\n"
                "Your verdict:"
            ),
        ),
    ]
)

# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------


def _make_summarise_node(llm: BaseChatModel):
    chain = _SUMMARISE_PROMPT | llm

    def summarise(state: SummarisationState) -> dict:
        feedback_section = (
            f"Previous attempt was rejected. Feedback: {state['feedback']}\nPlease correct.\n\n"
            if state["feedback"]
            else ""
        )
        response = chain.invoke(
            {
                "title": state["title"],
                "a2_text": state["a2_text"],
                "feedback_section": feedback_section,
            },
            config={"callbacks": [langfuse_handler]},
        )
        c = response.content  # type: ignore
        text = (c if isinstance(c, str) else c[0]["text"]).strip()  # type: ignore
        return {
            "summary": text,
            "attempts": state["attempts"] + 1,
        }

    return summarise


def _make_judge_node(llm: BaseChatModel):
    chain = _JUDGE_PROMPT | llm

    def judge(state: SummarisationState) -> dict:
        response = chain.invoke(
            {
                "title": state["title"],
                "a2_text": state["a2_text"],
                "summary": state["summary"],
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


def _should_retry(state: SummarisationState) -> Literal["summarise", "__end__"]:
    if state["judgment"] == "approved" or state["attempts"] >= 3:
        return "__end__"
    return "summarise"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------


def build_summariser_graph(llm: BaseChatModel):
    """Return a compiled LangGraph that summarises one chapter at A2 level."""
    builder: StateGraph = StateGraph(SummarisationState)

    builder.add_node("summarise", _make_summarise_node(llm))
    builder.add_node("judge", _make_judge_node(llm))

    builder.set_entry_point("summarise")
    builder.add_edge("summarise", "judge")
    builder.add_conditional_edges("judge", _should_retry)

    return builder.compile()


# ---------------------------------------------------------------------------
# Public runner
# ---------------------------------------------------------------------------


def summarise_chapter(
    graph,
    chapter_id: str,
    title: str,
    a2_text: str,
) -> SummarisationState:
    """
    Run the summarisation graph for a single chapter.

    Returns
    -------
    SummarisationState
        The final state; ``summary`` holds the A2-level summary.
        Falls back to whatever the last attempt produced if all attempts are rejected.
    """
    initial_state: SummarisationState = {
        "chapter_id": chapter_id,
        "title": title,
        "a2_text": a2_text,
        "summary": "",
        "judgment": "",
        "feedback": "",
        "feedback_history": [],
        "attempts": 0,
    }
    final_state: SummarisationState = graph.invoke(initial_state)
    return final_state
