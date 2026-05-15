import requests
import psycopg2
import boto3
from datetime import datetime, timezone

DB_PIPELINE = {
    "host": "localhost",
    "port": 5433,
    "dbname": "gestao_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

DB_OPERATIONAL = {
    "host": "localhost",
    "port": 5433,
    "dbname": "gestao_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

BUCKET_RAW = "bronze"
PROCESS_NAME = "etl_dados"


def detect_format(url: str, content: bytes) -> str:
    url_lower = url.lower().split("?")[0]
    if url_lower.endswith(".csv"):
        return "csv"
    if url_lower.endswith(".json"):
        return "json"
    if url_lower.endswith(".xlsx") or url_lower.endswith(".xls"):
        return "excel"
    if url_lower.endswith(".xml"):
        return "xml"
    if url_lower.endswith(".zip"):
        return "zip"
    # Fallback: sniff content
    try:
        snippet = content[:20]
        if snippet.lstrip().startswith(b"{") or snippet.lstrip().startswith(b"["):
            return "json"
        if b"," in snippet or b";" in snippet:
            return "csv"
    except Exception:
        pass
    return "json"


def main():
    print("A correr bronze...")

    s3 = boto3.client("s3", **MINIO_CONFIG)
    conn_pipe = psycopg2.connect(**DB_PIPELINE)
    cur_pipe  = conn_pipe.cursor()
    conn_op   = psycopg2.connect(**DB_OPERATIONAL)
    cur_op    = conn_op.cursor()

    # Garantir que o bucket existe
    try:
        s3.head_bucket(Bucket=BUCKET_RAW)
    except Exception:
        s3.create_bucket(Bucket=BUCKET_RAW)

    # Obter last_run (NULL na BD é tratado como primeira execução)
    cur_pipe.execute("SELECT last_run FROM etl_data WHERE process_name = %s", (PROCESS_NAME,))
    row = cur_pipe.fetchone()
    row_val = row[0] if row else None
    last_run = row_val if row_val is not None else datetime(2000, 1, 1)

    # Blacklist: file_ids com erro nesta fase desde o último run
    # Limitar a last_run para permitir retry em runs subsequentes
    cur_pipe.execute("""
        SELECT DISTINCT file_id FROM etl_logs_dados
        WHERE step = 'ingest_raw' AND file_id IS NOT NULL
          AND log_time > %s
    """, (last_run,))
    blacklist = {r[0] for r in cur_pipe.fetchall()}

    # Buscar novos registos
    cur_op.execute("""
        SELECT file_id, report_id, file_url, extract_function, created_at
        FROM op_data
        WHERE created_at > %s
        ORDER BY file_id ASC
    """, (last_run,))
    rows = cur_op.fetchall()

    if not rows:
        print("Sem novos ficheiros para ingerir.")
        cur_pipe.close(); conn_pipe.close()
        cur_op.close();   conn_op.close()
        return

    session = requests.Session()
    ok_count = 0
    err_count = 0

    for file_id, report_id, file_url, extract_function, created_at in rows:

        if file_id in blacklist:
            print(f"[SKIP] Blacklist: file_id={file_id}")
            continue

        # Ficheiro já carregado via upload — já está no MinIO
        if not file_url:
            print(f"[SKIP] Sem URL (upload direto): file_id={file_id}")
            ok_count += 1
            continue

        try:
            response = session.get(file_url, timeout=30)
            response.raise_for_status()
            content = response.content

            fmt = detect_format(file_url, content)
            key = str(file_id)

            created_str = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)

            s3.put_object(
                Bucket=BUCKET_RAW,
                Key=key,
                Body=content,
                Metadata={
                    "report_id": str(report_id) if report_id is not None else "",
                    "extract_function": extract_function or "",
                    "created_at": created_str,
                },
            )
            print(f"[OK]   file_id={file_id}  ({fmt})")
            ok_count += 1

        except Exception as e:
            cur_pipe.execute("""
                INSERT INTO etl_logs_dados (file_id, step, error_message)
                VALUES (%s, %s, %s)
            """, (file_id, "ingest_raw", str(e)))
            print(f"[ERRO] file_id={file_id}  {e}")
            err_count += 1

    conn_pipe.commit()
    cur_pipe.close(); conn_pipe.close()
    cur_op.close();   conn_op.close()
    print(f"ingest_raw concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    main()
