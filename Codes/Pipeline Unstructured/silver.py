from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from langchain_community.vectorstores import PGVector
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
load_dotenv()

from minio import Minio
from minio.error import S3Error
import pdfplumber

import io
import os
import uuid
import psycopg2
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

MINIO_SETTINGS = {
    "endpoint":   "localhost:9002",
    "access_key": "admin",
    "secret_key": "admin123",
    "secure":     False,
    "bucket":     "bronze-unstructured",
}

DB_VECTOR = {
    "host":     "localhost",
    "port":     5433,
    "database": "vector_db",
    "user":     "projeto_utilizador",
    "password": "projeto",
}

DB_GESTAO = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "gestao_db",
    "user":     "projeto_utilizador",
    "password": "projeto",
}

COLLECTION_NAME   = "documents"
CHUNK_SIZE        = 500
CHUNK_OVERLAP     = 50
EMBED_BATCH_SIZE  = 200  # chunks por chamada à API de embeddings
INSERT_BATCH_SIZE = 50   # chunks por lote de inserção na BD


# ── Fornecedor de embeddings ───────────────────────────────


def _get_embedding_provider() -> str:
    """Lê o fornecedor de embeddings da BD; fallback para variável de ambiente ou 'ollama'."""
    try:
        conn = psycopg2.connect(**DB_GESTAO)
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_settings (
                    key   VARCHAR(100) PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            conn.commit()
            cur.execute("SELECT value FROM pipeline_settings WHERE key = 'embedding_provider'")
            row = cur.fetchone()
            cur.close()
            return row[0] if row else os.getenv("EMBEDDING_PROVIDER", "ollama")
        finally:
            conn.close()
    except Exception:
        return os.getenv("EMBEDDING_PROVIDER", "ollama")


def get_embedding_function(provider: str | None = None):
    """Devolve a função de embeddings para o fornecedor indicado."""
    if provider is None:
        provider = _get_embedding_provider()
    if provider == "google":
        from langchain_google_genai import GoogleGenerativeAIEmbeddings
        return GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-001")
    else:
        try:
            from langchain_ollama import OllamaEmbeddings
        except ImportError:
            from langchain_community.embeddings import OllamaEmbeddings
        return OllamaEmbeddings(model="mxbai-embed-large")


# ── Utilitários de BD ──────────────────────────────────────


def _log_to_etl(file_name: str, step: str, error_message: str, report_id=None):
    try:
        conn = psycopg2.connect(**DB_GESTAO)
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                (report_id, file_name, step, error_message),
            )
            conn.commit()
            cur.close()
        finally:
            conn.close()
    except Exception:
        pass


def _mark_status(report_id: int, status: str, error: str | None):
    try:
        conn = psycopg2.connect(**DB_GESTAO)
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE op_report SET pipeline_status = %s WHERE report_id = %s",
                (status, report_id),
            )
            conn.commit()
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        print(f"[AVISO] Não foi possível atualizar status report_id={report_id}: {e}")


def _get_already_indexed(file_names: list[str]) -> set:
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_VECTOR["host"],
            port=DB_VECTOR["port"],
            dbname=DB_VECTOR["database"],
            user=DB_VECTOR["user"],
            password=DB_VECTOR["password"],
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT cmetadata->>'source'
            FROM langchain_pg_embedding
            WHERE cmetadata->>'source' = ANY(%s)
        """, (file_names,))
        indexed = {row[0] for row in cur.fetchall()}
        cur.close()
        return indexed
    except Exception as e:
        msg = f"Não foi possível consultar documentos já indexados: {e}"
        print(f"[AVISO] {msg}")
        _log_to_etl("_get_already_indexed", "silver", msg)
        return set()
    finally:
        if conn:
            conn.close()


def _delete_chunks_for_file(file_name: str):
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_VECTOR["host"],
            port=DB_VECTOR["port"],
            dbname=DB_VECTOR["database"],
            user=DB_VECTOR["user"],
            password=DB_VECTOR["password"],
        )
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM langchain_pg_embedding WHERE cmetadata->>'source' = %s",
            (file_name,),
        )
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        if deleted > 0:
            print(f"[LIMPEZA] {deleted} chunks parciais removidos para {file_name}")
    except Exception as e:
        print(f"[AVISO] Não foi possível limpar chunks de {file_name}: {e}")
    finally:
        if conn:
            conn.close()


# ── Extracção de texto ─────────────────────────────────────


def _table_to_text(table: list[list]) -> str:
    rows = []
    for row in table:
        cells = [str(cell).strip() if cell is not None else "" for cell in row]
        rows.append(" | ".join(cells))
    return "\n".join(rows)


def load_new_documents(client: Minio, new_names: list[str], report_id: int | None = None) -> list:
    documents = []
    for name in new_names:
        print(f"A carregar: {name}")
        response = client.get_object(MINIO_SETTINGS["bucket"], name)
        try:
            pdf_bytes = io.BytesIO(response.read())
        finally:
            response.close()
            response.release_conn()

        try:
            with pdfplumber.open(pdf_bytes) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    parts = []

                    found_tables = page.find_tables()
                    if found_tables:
                        table_bboxes = [t.bbox for t in found_tables]

                        def not_in_table(obj):
                            for bbox in table_bboxes:
                                if (obj.get("x0", 0)    >= bbox[0] - 1
                                        and obj.get("top", 0)    >= bbox[1] - 1
                                        and obj.get("x1", 0)     <= bbox[2] + 1
                                        and obj.get("bottom", 0) <= bbox[3] + 1):
                                    return False
                            return True

                        text = page.filter(not_in_table).extract_text() or ""
                    else:
                        text = page.extract_text() or ""

                    if text.strip():
                        parts.append(text.strip())

                    for table in page.extract_tables():
                        table_text = _table_to_text(table)
                        if table_text.strip():
                            parts.append(f"[TABELA]\n{table_text}")

                    combined = "\n\n".join(parts)
                    if combined.strip():
                        documents.append(Document(
                            page_content=combined,
                            metadata={"source": name, "page": page_num},
                        ))
        except Exception as e:
            msg = f"Erro ao processar PDF: {e}"
            print(f"  {name}: {msg}")
            _log_to_etl(name, "silver", msg, report_id)

    return documents


def split_documents(documents: list) -> list:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        is_separator_regex=False,
    )
    return splitter.split_documents(documents)


def assign_chunk_ids(chunks: list) -> list:
    last_page_id = None
    idx = 0
    for chunk in chunks:
        source  = chunk.metadata.get("source") or "unknown"
        page    = chunk.metadata.get("page")
        page    = page if page is not None else "unknown"
        page_id = f"{source}:{page}"
        if page_id == last_page_id:
            idx += 1
        else:
            idx = 0
        chunk.metadata["id"] = f"{page_id}:{idx}"
        last_page_id = page_id
    return chunks


# ── Inserção vetorial ──────────────────────────────────────


def _make_connection_string() -> str:
    return (
        f"postgresql+psycopg2://{DB_VECTOR['user']}:{DB_VECTOR['password']}"
        f"@{DB_VECTOR['host']}:{DB_VECTOR['port']}/{DB_VECTOR['database']}"
        f"?keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=5"
    )


def add_to_pgvector(chunks: list, file_name: str = "unknown", provider: str = "ollama"):
    """Insere chunks na BD vetorial em duas fases separadas:
    1. Gerar todos os embeddings (sem ligação DB aberta — evita timeout por idle).
    2. Inserir vectores pré-calculados em lotes pequenos com ligação fresca por lote.
    """
    if not chunks:
        return

    embedding_fn      = get_embedding_function(provider)
    connection_string = _make_connection_string()

    texts     = [c.page_content for c in chunks]
    metadatas = [c.metadata for c in chunks]
    ids       = [c.metadata.get("id") or str(uuid.uuid4()) for c in chunks]

    # Fase 1 — embeddings (sem ligação à BD)
    print(f"  A gerar embeddings para {len(texts)} chunks...")
    all_embeddings: list = []
    for i in range(0, len(texts), EMBED_BATCH_SIZE):
        sub = texts[i : i + EMBED_BATCH_SIZE]
        all_embeddings.extend(embedding_fn.embed_documents(sub))

    # Fase 2 — inserção com NullPool: cada add_embeddings abre e fecha a sua própria
    # ligação sem deixar nenhuma idle → elimina o TCP abort do Windows (0x00002745).
    # Um único engine/store por documento evita a explosão de conexões que destruía o Postgres.
    store = PGVector(
        connection_string=connection_string,
        embedding_function=embedding_fn,
        collection_name=COLLECTION_NAME,
        pre_delete_collection=False,
        engine_args={"poolclass": NullPool},
    )
    total = 0
    for i in range(0, len(texts), INSERT_BATCH_SIZE):
        batch_num = i // INSERT_BATCH_SIZE + 1
        s = slice(i, i + INSERT_BATCH_SIZE)
        try:
            store.add_embeddings(
                texts=texts[s],
                embeddings=all_embeddings[s],
                metadatas=metadatas[s],
                ids=ids[s],
            )
            total += len(texts[s])
        except Exception as e:
            msg = f"Falha ao inserir lote {batch_num}: {e}"
            print(f"  [ERRO] {msg}")
            _log_to_etl(file_name, "silver", msg)
            raise

    print(f"  {total} chunks inseridos.")


# ── Processamento por documento ────────────────────────────


def _process_one(
    report_id:      int,
    file_name:      str | None,
    already_indexed: frozenset,
    client:         Minio,
    provider:       str,
) -> bool:
    if file_name in already_indexed:
        _delete_chunks_for_file(file_name)

    try:
        documents = load_new_documents(client, [file_name], report_id)
        if not documents:
            raise ValueError("Sem páginas extraídas do PDF")
        chunks = split_documents(documents)
        chunks = assign_chunk_ids(chunks)
        add_to_pgvector(chunks, file_name, provider)
        _mark_status(report_id, "done", None)
        print(f"[OK]   report_id={report_id}  {file_name}  [{len(documents)} págs, {len(chunks)} chunks]")
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            _mark_status(report_id, "pending", None)
            print(f"[PENDING] report_id={report_id}  {file_name}: ficheiro não existe no bucket, reposto como PENDING")
        else:
            err_msg = str(e)
            _mark_status(report_id, "failed", err_msg)
            _log_to_etl(file_name, "silver", err_msg, report_id)
            print(f"[ERRO] report_id={report_id}  {file_name}: {err_msg}")
        return False
    except Exception as e:
        err_msg = str(e)
        _mark_status(report_id, "failed", err_msg)
        _log_to_etl(file_name, "silver", err_msg, report_id)
        print(f"[ERRO] report_id={report_id}  {file_name}: {err_msg}")
        return False


# ── Entry point ────────────────────────────────────────────


def main():
    print("A correr silver (ingest vectorial)...")

    provider = _get_embedding_provider()
    # Ollama é local e single-threaded; Google é remoto e suporta paralelismo
    max_workers = 1 if provider == "ollama" else 4
    print(f"Fornecedor de embeddings: {provider.upper()} (workers: {max_workers})")

    conn = psycopg2.connect(**DB_GESTAO)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT report_id, file_name
            FROM op_report
            WHERE pipeline_status = 'bronze'
            ORDER BY report_id
        """)
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    if not rows:
        print("Sem relatórios BRONZE para indexar.")
        return

    file_names      = [r[1] for r in rows if r[1] is not None]
    already_indexed = frozenset(_get_already_indexed(file_names))
    print(f"Documentos já indexados: {len(already_indexed)}")
    if already_indexed:
        print(f"Ficheiros com chunks parciais a limpar antes de re-indexar: {len(already_indexed)}")

    client = Minio(
        MINIO_SETTINGS["endpoint"],
        access_key=MINIO_SETTINGS["access_key"],
        secret_key=MINIO_SETTINGS["secret_key"],
        secure=MINIO_SETTINGS["secure"],
    )

    ok_count  = 0
    err_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_one, report_id, file_name, already_indexed, client, provider): report_id
            for report_id, file_name in rows
        }
        for future in as_completed(futures):
            report_id = futures[future]
            try:
                if future.result():
                    ok_count += 1
                else:
                    err_count += 1
            except Exception as e:
                print(f"[ERRO INESPERADO] report_id={report_id}: {e}")
                err_count += 1

    print(f"silver concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    main()
