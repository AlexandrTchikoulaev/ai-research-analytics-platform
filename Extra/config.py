# ── PostgreSQL ─────────────────────────────────────────────────────────────────
_PG_BASE = {
    "host":     "localhost",
    "port":     5433,
    "user":     "projeto_utilizador",
    "password": "projeto",
}

DB_WAREHOUSE = {**_PG_BASE, "dbname": "warehouse_db"}
DB_GESTAO    = {**_PG_BASE, "dbname": "gestao_db"}
DB_VECTOR    = {**_PG_BASE, "dbname": "vector_db"}

# ── MinIO ──────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = "localhost:9002"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_SECURE     = False

# Para clientes boto3 / aws_sdk
MINIO_CONFIG_BOTO3 = {
    "endpoint_url":        f"http://{MINIO_ENDPOINT}",
    "aws_access_key_id":    MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
}

# Para o cliente minio-python
MINIO_CONFIG = {
    "endpoint":   MINIO_ENDPOINT,
    "access_key": MINIO_ACCESS_KEY,
    "secret_key": MINIO_SECRET_KEY,
    "secure":     MINIO_SECURE,
}

# ── Buckets ────────────────────────────────────────────────────────────────────
BUCKET_BRONZE              = "bronze"
BUCKET_SILVER              = "silver"
BUCKET_BRONZE_UNSTRUCTURED = "bronze-unstructured"

MINIO_BUCKETS = [BUCKET_BRONZE, BUCKET_SILVER, BUCKET_BRONZE_UNSTRUCTURED]

# ── Processos ETL ──────────────────────────────────────────────────────────────
ETL_PROCESS_DADOS = "etl_dados"
ETL_PROCESS_PDFS  = "etl_pdfs"

# ── Ollama / RAG ───────────────────────────────────────────────────────────────
OLLAMA_BASE_URL    = "http://localhost:11434"
OLLAMA_MODEL       = "llama3"
OLLAMA_EMBED_MODEL = "mxbai-embed-large"
