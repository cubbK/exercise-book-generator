"""
Standalone LangGraph pipeline: translate a single book chapter to fluent Swedish.

Flow
----
[translate] ──► [judge] ──► END          (approved, or attempts ≥ 3)
      ▲               │
      └───────────────┘  (rejected with feedback)

Public API
----------
    graph  = build_translator_graph(llm)
    state  = translate_chapter(graph, chapter_id, title, raw_text, source_language)
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

# Model used for translation — requires a capable LLM for high-quality results
MODEL_NAME = "gemini-3.1-pro-preview"

# Initialize Langfuse client
langfuse = get_client()

# Initialize Langfuse CallbackHandler for Langchain (tracing)
langfuse_handler = CallbackHandler()

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


class TranslationState(TypedDict):
    chapter_id: str
    title: str
    raw_text: str
    source_language: str  # ISO 639-1 code of the source text
    swedish_text: str  # fluent Swedish translation
    judgment: str  # "approved" | "rejected"
    feedback: str  # latest rejection reason (empty if none)
    feedback_history: Annotated[
        list[str], operator.add
    ]  # all rejection reasons across attempts
    attempts: int


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_TRANSLATE_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are an expert translator. "
                "Translate the provided text into fluent, natural Swedish. "
                "Preserve the original meaning, tone, and structure. "
                "Return only the translated text — no explanations, no metadata."
            ),
        ),
        (
            "human",
            (
                "Feedback from previous attempt (if any):\n{feedback_section}\n"
                "Source language: {source_language}\n"
                "Chapter title: {title}\n\n"
                "Text to translate:\n{raw_text}\n\n"
                "Swedish translation:"
            ),
        ),
    ]
)

_JUDGE_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a quality-control judge for Swedish translations. "
                'Reply with exactly "approved" if the translation is accurate and written in fluent Swedish, '
                'or "rejected: <one-sentence reason>" if there are issues. '
                "No other output."
            ),
        ),
        (
            "human",
            (
                "Source language: {source_language}\n"
                "Chapter title: {title}\n\n"
                "Original text:\n{raw_text}\n\n"
                "Swedish translation:\n{swedish_text}\n\n"
                "Your verdict:"
            ),
        ),
    ]
)

# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------


def _make_translate_node(llm: BaseChatModel):
    chain = _TRANSLATE_PROMPT | llm

    def translate(state: TranslationState) -> dict:
        feedback_section = (
            f"Previous attempt was rejected. Feedback: {state['feedback']}\nPlease correct.\n\n"
            if state["feedback"]
            else ""
        )
        response = chain.invoke(
            {
                "source_language": state["source_language"],
                "title": state["title"],
                "raw_text": state["raw_text"],
                "feedback_section": feedback_section,
            },
            config={"callbacks": [langfuse_handler]},
        )
        c = response.content  # type: ignore
        text = (c if isinstance(c, str) else c[0]["text"]).strip()  # type: ignore
        return {
            "swedish_text": text,
            "attempts": state["attempts"] + 1,
        }

    return translate


def _make_judge_node(llm: BaseChatModel):
    chain = _JUDGE_PROMPT | llm

    def judge(state: TranslationState) -> dict:
        response = chain.invoke(
            {
                "source_language": state["source_language"],
                "title": state["title"],
                "raw_text": state["raw_text"],
                "swedish_text": state["swedish_text"],
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


def _should_retry(state: TranslationState) -> Literal["translate", "__end__"]:
    if state["judgment"] == "approved" or state["attempts"] >= 3:
        return "__end__"
    return "translate"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------


def build_translator_graph(llm: BaseChatModel):
    """Return a compiled LangGraph that translates one chapter to fluent Swedish."""
    builder: StateGraph = StateGraph(TranslationState)

    builder.add_node("translate", _make_translate_node(llm))
    builder.add_node("judge", _make_judge_node(llm))

    builder.set_entry_point("translate")
    builder.add_edge("translate", "judge")
    builder.add_conditional_edges("judge", _should_retry)

    return builder.compile()


# ---------------------------------------------------------------------------
# Public runner
# ---------------------------------------------------------------------------


def translate_chapter(
    graph,
    chapter_id: str,
    title: str,
    raw_text: str,
    source_language: str,
) -> TranslationState:
    """
    Run the translation graph for a single chapter.

    Returns
    -------
    TranslationState
        The final state; ``swedish_text`` holds the fluent Swedish translation.
        Falls back to whatever the last attempt produced if all attempts are rejected.
    """
    initial_state: TranslationState = {
        "chapter_id": chapter_id,
        "title": title,
        "raw_text": raw_text,
        "source_language": source_language,
        "swedish_text": "",
        "judgment": "",
        "feedback": "",
        "feedback_history": [],
        "attempts": 0,
    }
    final_state: TranslationState = graph.invoke(initial_state)
    return final_state
