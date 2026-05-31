"""
PDF Chat — RAG sobre relatórios indexados (vector_db).

Pipeline:
  1. Embedding da pergunta (Ollama: mxbai-embed-large | Google: text-embedding-004)
  2. Similarity search no PGVector (k=5)
  3. Geração de resposta (Ollama: mistral:latest | Google: gemini-2.0-flash)
  4. Devolve {answer, sources}
"""

import psycopg2
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.vectorstores import PGVector

# ── Config ─────────────────────────────────────────────────────────────────
_DB_VECTOR = {
    "host":     "localhost",
    "port":     5433,
    "database": "vector_db",
    "user":     "projeto_utilizador",
    "password": "projeto",
}

_DB_SETTINGS = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "gestao_db",
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


# ── Settings ────────────────────────────────────────────────────────────────
def _get_setting(key: str, default: str) -> str:
    try:
        conn = psycopg2.connect(**_DB_SETTINGS)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM pipeline_settings WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else default
        finally:
            conn.close()
    except Exception:
        return default


# ── Providers ───────────────────────────────────────────────────────────────
def _get_embedding_function():
    provider = _get_setting("embedding_provider", "ollama")
    if provider == "google":
        api_key = _get_setting("google_api_key", "")
        from langchain_google_genai import GoogleGenerativeAIEmbeddings
        return GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-001", google_api_key=api_key)
    from langchain_community.embeddings.ollama import OllamaEmbeddings
    return OllamaEmbeddings(model="mxbai-embed-large")


def _get_llm():
    provider = _get_setting("llm_provider", "ollama")
    if provider == "google":
        api_key = _get_setting("google_api_key", "")
        from langchain_google_genai import ChatGoogleGenerativeAI
        return ChatGoogleGenerativeAI(model="gemini-2.0-flash", google_api_key=api_key, temperature=0)
    from langchain_community.llms.ollama import Ollama
    return Ollama(model="mistral:latest", temperature=0)


# ── Core ───────────────────────────────────────────────────────────────────
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

    llm = _get_llm()
    result = llm.invoke(prompt)
    response_text = result.content if hasattr(result, "content") else str(result)

    sources = [doc.metadata.get("id") for doc, _score in results]
    return {"answer": response_text, "sources": sources}
