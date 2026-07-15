# Intelligent Reports & Data Repository

GenAI platform combining data engineering with generative AI to enable
natural-language exploration of both structured data (tabular
rankings/indicators in a data warehouse) and unstructured data (PDF
reports), through a dual-mode chatbot and interactive dashboards.

## Architecture

A Medallion architecture with two parallel Bronze/Silver pipelines feeding
the same serving layer:

```
Tabular files  -> Codes/Pipeline/              -> Bronze -> Silver -> Gold (warehouse_db)
PDF reports    -> Codes/Pipeline Unstructured/  -> Bronze -> Silver (embeddings in vector_db)
                                                                    |
                                                                    v
                                            Codes/Website/  (FastAPI + dual-mode chatbot)
```

**Tabular pipeline** ingests heterogeneous files (JSON/CSV/Excel) into
Bronze in MinIO. Rather than hand-writing a parser per format, Silver uses
an AI-assisted code-generation step (`silver_function_generator.py`): an
LLM (Ollama) generates the transformation function for a given file type,
validates it against the data, and falls back to a manual implementation
if generation or validation fails. Gold lands in a star-schema data
warehouse (`warehouse_db`).

**Unstructured pipeline** downloads PDF reports into Bronze, then indexes
them in Silver: chunking, embedding, and storing vectors in `vector_db`
(pgvector) via LangChain, ready for retrieval.

**Serving layer** exposes a dual-mode chatbot over FastAPI:
- **Data mode** (`chat_data.py`) — Text-to-SQL over the warehouse. Routes
  each question through tiers of increasing complexity (fixed Python for
  known metadata queries, LLM + template for simple lookups, full
  schema-aware prompting for complex ones), with RapidFuzz fuzzy matching
  to resolve country/indicator names against the schema.
- **Reports mode** (`chat_reports.py`) — RAG over the indexed PDFs:
  embeds the question, runs a similarity search against pgvector, and
  generates a grounded answer with citations back to source documents.

Both modes can run on either a local LLM (Ollama: `mistral`,
`mxbai-embed-large`) or Google Gemini (`gemini-2.0-flash`,
`text-embedding-004`), selectable per request.

The frontend (`Codes/Website/index.html` + Plotly.js) ships the chatbot
alongside interactive analytics dashboards over the same warehouse, and
runs as a desktop app via `pywebview` with a system-tray icon.

## Stack

Python · LangChain · FastAPI · PostgreSQL · pgvector · MinIO · Ollama ·
Google Gemini API · RapidFuzz · Plotly · Docker

## Running it

```
docker compose -f docker/docker-compose.yml up -d   # Postgres, MinIO
python Codes/Setup/setup.py                          # create databases/buckets
python start.py                                       # launch API + desktop window
```

`scripts/` (`Extra/Codes/`) provides `reset_pipeline.py` and
`delete_setup.py` to reset the pipeline state or tear the databases down
between runs.

## Structure

```
Codes/
  Pipeline/              tabular data: Bronze -> Silver (AI-generated + validated
                          transforms) -> Gold star schema
  Pipeline Unstructured/  PDF reports: Bronze -> Silver (chunk, embed, index)
  Setup/                  database/bucket provisioning
  Website/                FastAPI backend, dual-mode chatbot, dashboard frontend
App/                      desktop shortcut + icon generation
docker/                   Docker Compose for Postgres + MinIO
start.py / stop.py        desktop app entrypoints (system tray + webview window)
```
