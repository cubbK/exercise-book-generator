from __future__ import annotations

from dagster import ConfigurableResource
from langchain_google_genai import ChatGoogleGenerativeAI


class VertexAIResource(ConfigurableResource):
    """Vertex AI resource backed by ``ChatVertexAI``.

    Authentication uses Application Default Credentials (ADC), the same
    mechanism as BigQuery — no extra credentials required on GCP.
    """

    project: str
    location: str
    model_name: str = "gemini-2.0-flash"

    def get_llm(self) -> ChatGoogleGenerativeAI:
        return ChatGoogleGenerativeAI(
            model=self.model_name,
            project=self.project,
            location=self.location,
        )
