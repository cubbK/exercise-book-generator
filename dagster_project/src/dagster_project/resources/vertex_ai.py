from __future__ import annotations

from dagster import ConfigurableResource
from langchain_google_genai import ChatGoogleGenerativeAI


class VertexAIResource(ConfigurableResource):
    """Vertex AI resource backed by ``ChatGoogleGenerativeAI``.

    Authentication uses Application Default Credentials (ADC), the same
    mechanism as BigQuery — no extra credentials required on GCP.
    """

    project: str
    model_name: str = "gemini-3.1-flash-lite-preview"

    def get_llm(self, model_name: str | None = None) -> ChatGoogleGenerativeAI:
        return ChatGoogleGenerativeAI(
            model=model_name or self.model_name,
            project=self.project,
        )
