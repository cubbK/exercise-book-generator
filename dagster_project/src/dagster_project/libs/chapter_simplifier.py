"""
Standalone LangGraph pipeline: simplify a Swedish chapter to A2 level.

Flow
----
[simplify] ──► [judge] ──► END          (approved, or attempts ≥ 3)
      ▲               │
      └───────────────┘  (rejected with feedback)

Public API
----------
    graph  = build_simplifier_graph(llm)
    state  = simplify_chapter(graph, chapter_id, title, swedish_text)
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

# Model used for simplification
MODEL_NAME = "gemini-3.1-pro-preview"

# Initialize Langfuse client
langfuse = get_client()

# Initialize Langfuse CallbackHandler for Langchain (tracing)
langfuse_handler = CallbackHandler()

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


class SimplificationState(TypedDict):
    chapter_id: str
    title: str
    swedish_text: str  # fluent Swedish input
    a2_text: str  # simplified A2 Swedish output
    judgment: str  # "approved" | "rejected"
    feedback: str  # latest rejection reason (empty if none)
    feedback_history: Annotated[
        list[str], operator.add
    ]  # all rejection reasons across attempts
    attempts: int


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_SIMPLIFY_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are an expert in plain-language writing for Swedish language learners. "
                "Rewrite the provided Swedish text so it is suitable for A2-level learners. "
                "Guidelines:\n"
                "- Use simple sentences \n"
                "- Use only common, everyday Swedish vocabulary (A1–A2 level).\n"
                "- Replace complex words with simpler alternatives; explain unusual terms inline if necessary.\n"
                "- Preserve the core meaning and key facts of the original.\n"
                "- Keep the same topic structure and paragraph order.\n"
                "- Do NOT add new information or commentary.\n"
                "Return only the simplified Swedish text — no explanations, no metadata."
            ),
        ),
        (
            "human",
            (
                "Feedback from previous attempt (if any):\n{feedback_section}\n"
                "Chapter title: {title}\n\n"
                "Original Swedish text:\n{swedish_text}\n\n"
                "Simplified A2 Swedish text:"
            ),
        ),
    ]
)

_JUDGE_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a quality-control judge for Swedish A2-level simplified texts. "
                "Evaluate whether the simplified text meets A2 language learner standards:\n"
                "- Vocabulary is basic and common (A1–A2 CEFR level).\n"
                "- Sentences are simple.\n"
                "- Core meaning of the original is preserved.\n"
                'Reply with exactly "approved" if the text meets these criteria, '
                'or "rejected: <one-sentence reason>" if there are issues. '
                "No other output."
            ),
        ),
        (
            "human",
            (
                "Chapter title: {title}\n\n"
                "Original Swedish text:\n{swedish_text}\n\n"
                "Simplified A2 text:\n{a2_text}\n\n"
                "Your verdict:"
            ),
        ),
    ]
)

# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------


def _make_simplify_node(llm: BaseChatModel):
    chain = _SIMPLIFY_PROMPT | llm

    def simplify(state: SimplificationState) -> dict:
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
            "a2_text": text,
            "attempts": state["attempts"] + 1,
        }

    return simplify


def _make_judge_node(llm: BaseChatModel):
    chain = _JUDGE_PROMPT | llm

    def judge(state: SimplificationState) -> dict:
        response = chain.invoke(
            {
                "title": state["title"],
                "swedish_text": state["swedish_text"],
                "a2_text": state["a2_text"],
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


def _should_retry(state: SimplificationState) -> Literal["simplify", "__end__"]:
    if state["judgment"] == "approved" or state["attempts"] >= 3:
        return "__end__"
    return "simplify"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------


def build_simplifier_graph(llm: BaseChatModel):
    """Return a compiled LangGraph that simplifies one Swedish chapter to A2 level."""
    builder: StateGraph = StateGraph(SimplificationState)

    builder.add_node("simplify", _make_simplify_node(llm))
    builder.add_node("judge", _make_judge_node(llm))

    builder.set_entry_point("simplify")
    builder.add_edge("simplify", "judge")
    builder.add_conditional_edges("judge", _should_retry)

    return builder.compile()


# ---------------------------------------------------------------------------
# Public runner
# ---------------------------------------------------------------------------


def simplify_chapter(
    graph,
    chapter_id: str,
    title: str,
    swedish_text: str,
) -> SimplificationState:
    """
    Run the simplification graph for a single chapter.

    Returns
    -------
    SimplificationState
        The final state; ``a2_text`` holds the A2-level simplified Swedish text.
        Falls back to whatever the last attempt produced if all attempts are rejected.
    """
    initial_state: SimplificationState = {
        "chapter_id": chapter_id,
        "title": title,
        "swedish_text": swedish_text,
        "a2_text": "",
        "judgment": "",
        "feedback": "",
        "feedback_history": [],
        "attempts": 0,
    }
    final_state: SimplificationState = graph.invoke(initial_state)
    return final_state
