from dagster_project.lib.chapter_categorizer import (
    categorize_chapter,
    build_categorizer_graph,
)
from dagster_project.resources.vertex_ai import VertexAIResource

vertex = VertexAIResource(project="dan-learning-0929")
llm = vertex.get_llm()

result = categorize_chapter(
    graph=build_categorizer_graph(llm),
    chapter_id="1",
    title="The Great Gatsby",
    raw_text="In my younger and more vulnerable years my father gave me some advice that I’ve been turning over in my mind ever since. “Whenever you feel like criticizing any one,”",
)


print(result)
