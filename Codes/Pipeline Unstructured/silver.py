from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from langchain_community.embeddings.ollama import OllamaEmbeddings
from langchain_community.vectorstores import PGVector

from minio import Minio
import pdfplumber

import io
import psycopg2

MINIO_SETTINGS = {
    "endpoint": "localhost:9002",
    "access_key": "admin",
    "secret_key": "admin123",
    "secure": False,
    "bucket": "bronze-unstructured",
}

DB_VECTOR = {
    "host": "localhost",
    "port": 5433,
    "database": "vector_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

DB_GESTAO = {
    "host": "localhost",
    "port": 5433,
    "dbname": "gestao_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

COLLECTION_NAME = "documents"
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50
BATCH_SIZE = 50


def _log_to_etl(file_name: str, step: str, error_message: str, report_id=None):
    try:
        conn = psycopg2.connect(**DB_GESTAO)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
            (report_id, file_name, step, error_message),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass


def _mark_status(report_id: int, status: str, error: str | None):
    try:
        conn = psycopg2.connect(**DB_GESTAO)
        cur = conn.cursor()
        cur.execute(
            "UPDATE op_report SET pipeline_status = %s, pipeline_error = %s WHERE report_id = %s",
            (status, error, report_id),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[AVISO] Não foi possível atualizar status report_id={report_id}: {e}")


def get_embedding_function():
    return OllamaEmbeddings(model="mxbai-embed-large")


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
        _log_to_etl("silver", "silver", msg)
        return set()
    finally:
        if conn:
            conn.close()


def _table_to_text(table: list[list]) -> str:
    rows = []
    for row in table:
        cells = [str(cell).strip() if cell is not None else "" for cell in row]
        rows.append(" | ".join(cells))
    return "\n".join(rows)


def load_new_documents(new_names: list[str]) -> list:
    client = Minio(
        MINIO_SETTINGS["endpoint"],
        access_key=MINIO_SETTINGS["access_key"],
        secret_key=MINIO_SETTINGS["secret_key"],
        secure=MINIO_SETTINGS["secure"],
    )

    documents = []
    for name in new_names:
        print(f"A carregar: {name}")
        response = client.get_object(MINIO_SETTINGS["bucket"], name)
        pdf_bytes = io.BytesIO(response.read())
        response.close()
        response.release_conn()

        try:
            with pdfplumber.open(pdf_bytes) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    parts = []

                    # Texto fora das tabelas
                    found_tables = page.find_tables()
                    if found_tables:
                        table_bboxes = [t.bbox for t in found_tables]

                        def not_in_table(obj):
                            for bbox in table_bboxes:
                                if (obj.get("x0", 0) >= bbox[0] - 1
                                        and obj.get("top", 0) >= bbox[1] - 1
                                        and obj.get("x1", 0) <= bbox[2] + 1
                                        and obj.get("bottom", 0) <= bbox[3] + 1):
                                    return False
                            return True

                        text = page.filter(not_in_table).extract_text() or ""
                    else:
                        text = page.extract_text() or ""

                    if text.strip():
                        parts.append(text.strip())

                    # Tabelas convertidas para texto estruturado
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
            _log_to_etl(name, "silver", msg)

    print(f"{len(documents)} páginas novas carregadas.")
    return documents


def split_documents(documents: list) -> list:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        is_separator_regex=False,
    )
    chunks = splitter.split_documents(documents)
    for chunk in chunks:
        if len(chunk.page_content) > 1000:
            chunk.page_content = chunk.page_content[:1000]
    return chunks


def assign_chunk_ids(chunks: list) -> list:
    last_page_id = None
    idx = 0
    for chunk in chunks:
        source = chunk.metadata.get("source")
        page = chunk.metadata.get("page")
        page_id = f"{source}:{page}"
        if page_id == last_page_id:
            idx += 1
        else:
            idx = 0
        chunk.metadata["id"] = f"{page_id}:{idx}"
        last_page_id = page_id
    return chunks


def add_to_pgvector(chunks: list):
    connection_string = (
        f"postgresql://{DB_VECTOR['user']}:{DB_VECTOR['password']}"
        f"@{DB_VECTOR['host']}:{DB_VECTOR['port']}/{DB_VECTOR['database']}"
    )
    embedding_fn = get_embedding_function()

    store = PGVector(
        connection_string=connection_string,
        embedding_function=embedding_fn,
        collection_name=COLLECTION_NAME,
        pre_delete_collection=False,
    )

    for i in range(0, len(chunks), BATCH_SIZE):
        batch = chunks[i:i + BATCH_SIZE]
        try:
            store.add_documents(batch)
            print(f"  Lote {i // BATCH_SIZE + 1}: {len(batch)} chunks inseridos")
        except Exception as e:
            msg = f"Falha ao inserir lote {i // BATCH_SIZE + 1}: {e}"
            print(f"  [ERRO] {msg}")
            _log_to_etl(f"batch_{i // BATCH_SIZE + 1}", "silver", msg)
            raise

    print(f"{len(chunks)} chunks inseridos no total.")


def main():
    print("A correr silver (ingest vectorial)...")

    conn = psycopg2.connect(**DB_GESTAO)
    cur  = conn.cursor()
    cur.execute("""
        SELECT report_id, file_name
        FROM op_report
        WHERE pipeline_status = 'BRONZE_OK'
        ORDER BY report_id
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        print("Sem relatórios BRONZE_OK para indexar.")
        return

    file_names      = [r[1] for r in rows]
    already_indexed = _get_already_indexed(file_names)
    print(f"Documentos já indexados: {len(already_indexed)}")

    for report_id, file_name in rows:
        if file_name in already_indexed:
            _mark_status(report_id, "DONE", None)
            print(f"[SKIP] report_id={report_id}  {file_name}  [já indexado]")
            continue

        try:
            documents = load_new_documents([file_name])
            if not documents:
                raise ValueError("Sem páginas extraídas do PDF")
            chunks = split_documents(documents)
            chunks = assign_chunk_ids(chunks)
            add_to_pgvector(chunks)
            _mark_status(report_id, "DONE", None)
            print(f"[OK]   report_id={report_id}  {file_name}")
        except Exception as e:
            err_msg = str(e)
            _mark_status(report_id, "FAILED", err_msg)
            _log_to_etl(file_name, "silver", err_msg, report_id)
            print(f"[ERRO] report_id={report_id}  {file_name}: {err_msg}")

    print("silver concluído.")


if __name__ == "__main__":
    main()
