"""
Orquestrador do pipeline de dados não estruturados (PDFs).
Executa sequencialmente:
  1. ingest_unstructured — descarrega PDFs para o bucket unstructured
  2. ingest_vectorialdb  — processa PDFs e indexa embeddings na BD vetorial
"""
import sys
import os
import psycopg2
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "pipeline_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

PROCESS_NAME = "etl_pdfs"


def update_timestamp():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        UPDATE etl_data SET last_run = CURRENT_TIMESTAMP
        WHERE process_name = %s
    """, (PROCESS_NAME,))
    conn.commit()
    cur.close()
    conn.close()
    print("Timestamp etl_pdfs atualizado.")


def run_step(label: str, fn):
    print(f"\n{'='*50}")
    print(f" {label}")
    print(f"{'='*50}")
    try:
        result = fn()
        print(f"[OK] {label} concluído.")
        return result
    except Exception as e:
        print(f"[ERRO] {label} falhou: {e}")
        raise


def run_pipeline():
    import ingest_unstructured
    import ingest_vectorialdb

    print("\n PIPELINE DE PDFs INICIADO")
    print(f" {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 1. Descarregar PDFs para MinIO
    run_step("1/2 — ingest_unstructured", ingest_unstructured.main)

    # 2. Indexar embeddings na BD vetorial
    run_step("2/2 — ingest_vectorialdb", ingest_vectorialdb.main)

    # Atualizar timestamp
    update_timestamp()

    print("\n PIPELINE DE PDFs CONCLUÍDO COM SUCESSO")


if __name__ == "__main__":
    run_pipeline()
