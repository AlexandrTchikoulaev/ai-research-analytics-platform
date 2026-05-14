"""
PDF Chat — RAG sobre relatórios indexados (vector_db).

Pipeline:
  1. Embedding da pergunta com mxbai-embed-large (Ollama)
  2. Similarity search no PGVector (k=5)
  3. Geração de resposta com qwen2.5:7b (Ollama)
  4. Devolve {answer, sources}
"""

from langchain_core.prompts import ChatPromptTemplate
from langchain_community.llms.ollama import Ollama
from langchain_community.vectorstores import PGVector
from langchain_community.embeddings.ollama import OllamaEmbeddings

# ── Config ─────────────────────────────────────────────────────────────────
_DB_VECTOR = {
    "host":     "localhost",
    "port":     5433,
    "database": "vector_db",
    "user":     "projeto_utilizador",
    "password": "projeto",
}

_PROMPT_TEMPLATE = """
You are a helpful assistant. Answer the question based only on the following context.
Always respond in European Portuguese, regardless of the language of the context.
If the context does not contain enough information to answer, say so in Portuguese.

Context:
{context}

---

Question: {question}
Answer (in Portuguese):"""


# ── Core ───────────────────────────────────────────────────────────────────
def _get_embedding_function():
    return OllamaEmbeddings(model="mxbai-embed-large")


def query_rag(query_text: str) -> dict:
    embedding_function = _get_embedding_function()
    connection_string = (
        f"postgresql+psycopg2://{_DB_VECTOR['user']}:{_DB_VECTOR['password']}"
        f"@{_DB_VECTOR['host']}:{_DB_VECTOR['port']}/{_DB_VECTOR['database']}"
    )
    db = PGVector(
        connection_string=connection_string,
        embedding_function=embedding_function,
        collection_name="documents",
    )
    results = db.similarity_search_with_score(query_text, k=5)
    if not results:
        return {"answer": "Não encontrei informação relevante nos documentos indexados.", "sources": []}

    context_text = "\n\n---\n\n".join([doc.page_content for doc, _score in results])
    prompt_template = ChatPromptTemplate.from_template(_PROMPT_TEMPLATE)
    prompt = prompt_template.format(context=context_text, question=query_text)
    model = Ollama(model="qwen2.5:7b")
    response_text = model.invoke(prompt)
    sources = [doc.metadata.get("id") for doc, _score in results]
    return {"answer": response_text, "sources": sources}
