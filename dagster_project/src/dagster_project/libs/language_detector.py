"""
Standalone LangGraph pipeline: detect the language of a single book chapter.

Flow
----
[detect] ──► [judge] ──► END          (approved, or attempts ≥ 3)
    ▲               │
    └───────────────┘  (rejected with feedback)

Public API
----------
    graph  = build_detector_graph(llm)
    state  = detect_language(graph, chapter_id, title, raw_text)
"""

from __future__ import annotations

import operator
import re
from typing import Annotated, Literal

from langchain_core.language_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import StateGraph
from typing_extensions import TypedDict

from langfuse import get_client
from langfuse.langchain import CallbackHandler

# Initialize Langfuse client
langfuse = get_client()

# Initialize Langfuse CallbackHandler for Langchain (tracing)
langfuse_handler = CallbackHandler()

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


class LanguageDetectionState(TypedDict):
    chapter_id: str
    title: str
    raw_text: str
    detected_language: str  # ISO 639-1 code, e.g. "sv", "en", "de"
    judgment: str  # "approved" | "rejected"
    feedback: str  # latest rejection reason (empty if none)
    feedback_history: Annotated[
        list[str], operator.add
    ]  # all rejection reasons across attempts
    attempts: int


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_DETECT_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a language-identification expert. "
                "Respond with exactly ONE lowercase ISO 639-1 language code (e.g. sv, en, de, fr, es). "
                "No punctuation, no explanation."
            ),
        ),
        (
            "human",
            (
                "Feedback from previous attempt (if any):\n{feedback_section}\n"
                "Chapter title: {title}\n\n"
                "Text:\n{raw_text}\n\n"
                "Language code:"
            ),
        ),
    ]
)

_JUDGE_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a quality-control judge for language detection. "
                'Reply with exactly "approved" if the language code is correct, '
                'or "rejected: <one-sentence reason>" if it is wrong. '
                "No other output."
            ),
        ),
        (
            "human",
            (
                "Chapter title: {title}\n\n"
                "Text:\n{raw_text}\n\n"
                "Detected language code: {detected_language}\n\n"
                "Your verdict:"
            ),
        ),
    ]
)

# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

# ISO 639-1 codes are exactly 2 lowercase letters. Reject anything else
# (e.g. "swedish", "unknown") and fall back to "und" (undetermined).
_ISO_639_1_RE = re.compile(r"^[a-z]{2}$")


def _make_detect_node(llm: BaseChatModel):
    chain = _DETECT_PROMPT | llm

    def detect(state: LanguageDetectionState) -> dict:
        feedback_section = (
            f"Previous attempt was rejected. Feedback: {state['feedback']}\nPlease correct.\n\n"
            if state["feedback"]
            else ""
        )
        response = chain.invoke(
            {
                "title": state["title"],
                "raw_text": state["raw_text"],
                "feedback_section": feedback_section,
            },
            config={"callbacks": [langfuse_handler]},
        )
        c = response.content  # type: ignore
        raw = (c if isinstance(c, str) else c[0]["text"]).strip().lower()  # type: ignore
        # Accept only well-formed ISO 639-1 codes; fall back to "und" (undetermined)
        code = raw if _ISO_639_1_RE.match(raw) else "und"
        return {
            "detected_language": code,
            "attempts": state["attempts"] + 1,
        }

    return detect


def _make_judge_node(llm: BaseChatModel):
    chain = _JUDGE_PROMPT | llm

    def judge(state: LanguageDetectionState) -> dict:
        response = chain.invoke(
            {
                "title": state["title"],
                "raw_text": state["raw_text"],
                "detected_language": state["detected_language"],
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


def _should_retry(state: LanguageDetectionState) -> Literal["detect", "__end__"]:
    if state["judgment"] == "approved" or state["attempts"] >= 3:
        return "__end__"
    return "detect"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------


def build_detector_graph(llm: BaseChatModel):
    """Return a compiled LangGraph that detects the language of one chapter."""
    builder: StateGraph = StateGraph(LanguageDetectionState)

    builder.add_node("detect", _make_detect_node(llm))
    builder.add_node("judge", _make_judge_node(llm))

    builder.set_entry_point("detect")
    builder.add_edge("detect", "judge")
    builder.add_conditional_edges("judge", _should_retry)

    return builder.compile()


# ---------------------------------------------------------------------------
# Public runner
# ---------------------------------------------------------------------------

# Truncate to keep prompt costs manageable (mirrors chapter_categorizer pattern)
_TEXT_CHARS = 800


def detect_language(
    graph,
    chapter_id: str,
    title: str,
    raw_text: str,
) -> LanguageDetectionState:
    """
    Run the language-detection graph for a single chapter.

    Returns
    -------
    LanguageDetectionState
        The final state; ``detected_language`` holds the ISO 639-1 code.
        Falls back to ``"und"`` (undetermined) if all attempts are rejected.
    """
    initial_state: LanguageDetectionState = {
        "chapter_id": chapter_id,
        "title": title,
        "raw_text": raw_text[:_TEXT_CHARS],
        "detected_language": "",
        "judgment": "",
        "feedback": "",
        "feedback_history": [],
        "attempts": 0,
    }
    final_state: LanguageDetectionState = graph.invoke(initial_state)
    if final_state["judgment"] == "rejected":
        final_state["detected_language"] = "und"
    return final_state
