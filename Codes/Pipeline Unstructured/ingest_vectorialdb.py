from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from langchain_community.embeddings.ollama import OllamaEmbeddings
from langchain_community.vectorstores import PGVector

from minio import Minio
from pypdf import PdfReader

import io
import psycopg2

MINIO_SETTINGS = {
    "endpoint": "localhost:9002",
    "access_key": "admin",
    "secret_key": "admin123",
    "secure": False,
    "bucket": "bronze-unstructured",
}

DB_SETTINGS = {
    "host": "localhost",
    "port": 5433,
    "database": "vector_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

COLLECTION_NAME = "documents"
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50
BATCH_SIZE = 50


def get_embedding_function():
    return OllamaEmbeddings(model="mxbai-embed-large")


def get_already_indexed() -> set:
    """Devolve o conjunto de nomes de ficheiros já indexados na BD vetorial."""
    conn = psycopg2.connect(
        host=DB_SETTINGS["host"],
        port=DB_SETTINGS["port"],
        dbname=DB_SETTINGS["database"],
        user=DB_SETTINGS["user"],
        password=DB_SETTINGS["password"],
    )
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT cmetadata->>'source'
            FROM langchain_pg_embedding
            WHERE cmetadata->>'source' IS NOT NULL
        """)
        indexed = {row[0] for row in cur.fetchall()}
    except Exception:
        # Tabela pode ainda não existir
        indexed = set()
    cur.close()
    conn.close()
    return indexed


def load_new_documents(already_indexed: set) -> list:
    client = Minio(
        MINIO_SETTINGS["endpoint"],
        access_key=MINIO_SETTINGS["access_key"],
        secret_key=MINIO_SETTINGS["secret_key"],
        secure=MINIO_SETTINGS["secure"],
    )

    documents = []
    objects = client.list_objects(MINIO_SETTINGS["bucket"], recursive=True)

    for obj in objects:
        name = obj.object_name

        if name in already_indexed:
            print(f"[SKIP] Já indexado: {name}")
            continue

        print(f"A carregar: {name}")
        response = client.get_object(MINIO_SETTINGS["bucket"], name)
        pdf_bytes = io.BytesIO(response.read())
        response.close()
        response.release_conn()

        try:
            reader = PdfReader(pdf_bytes)
            for page_num, page in enumerate(reader.pages):
                text = page.extract_text() or ""
                if text.strip():
                    documents.append(Document(
                        page_content=text,
                        metadata={"source": name, "page": page_num},
                    ))
        except Exception as e:
            print(f"  Erro ao processar {name}: {e}")

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
        f"postgresql://{DB_SETTINGS['user']}:{DB_SETTINGS['password']}"
        f"@{DB_SETTINGS['host']}:{DB_SETTINGS['port']}/{DB_SETTINGS['database']}"
    )
    embedding_fn = get_embedding_function()

    # Inserir em lotes de BATCH_SIZE
    for i in range(0, len(chunks), BATCH_SIZE):
        batch = chunks[i:i + BATCH_SIZE]
        PGVector.from_documents(
            documents=batch,
            embedding=embedding_fn,
            connection_string=connection_string,
            collection_name=COLLECTION_NAME,
            pre_delete_collection=False,
        )
        print(f"  Lote {i // BATCH_SIZE + 1}: {len(batch)} chunks inseridos")

    print(f"{len(chunks)} chunks inseridos no total.")


def main():
    print("A correr ingest_vectorialdb...")

    already_indexed = get_already_indexed()
    print(f"Documentos já indexados: {len(already_indexed)}")

    documents = load_new_documents(already_indexed)

    if not documents:
        print("Sem novos documentos para indexar.")
        return

    chunks = split_documents(documents)
    chunks = assign_chunk_ids(chunks)
    add_to_pgvector(chunks)
    print("ingest_vectorialdb concluído.")


if __name__ == "__main__":
    main()
