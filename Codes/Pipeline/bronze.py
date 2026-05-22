import requests
import psycopg2
import boto3

DB_CONFIG = {
    "host": "localhost", "port": 5433, "dbname": "gestao_db",
    "user": "projeto_utilizador", "password": "projeto",
}

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

BUCKET_RAW = "bronze"


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
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        s3.head_bucket(Bucket=BUCKET_RAW)
    except Exception:
        s3.create_bucket(Bucket=BUCKET_RAW)

    # Lock rows PENDING e marca imediatamente como PROCESSING (liberta o lock de seguida)
    cur.execute("""
        SELECT file_id, report_id, file_url
        FROM op_data
        WHERE pipeline_status = 'PENDING'
        ORDER BY file_id
        FOR UPDATE SKIP LOCKED
    """)
    rows = cur.fetchall()

    if not rows:
        print("Sem ficheiros PENDING para ingerir.")
        cur.close(); conn.close()
        return

    file_ids = [r[0] for r in rows]
    cur.execute(
        "UPDATE op_data SET pipeline_status = 'PROCESSING' WHERE file_id = ANY(%s)",
        (file_ids,)
    )
    conn.commit()

    session = requests.Session()
    ok_count = 0
    err_count = 0

    for file_id, report_id, file_url in rows:

        # Ficheiro já carregado via upload — já está no MinIO
        if not file_url:
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'BRONZE_OK' WHERE file_id = %s",
                (file_id,)
            )
            conn.commit()
            print(f"[OK]   file_id={file_id}  [upload direto]")
            ok_count += 1
            continue

        try:
            response = session.get(file_url, timeout=30)
            response.raise_for_status()
            content = response.content

            fmt = detect_format(file_url, content)
            key = str(file_id)

            s3.put_object(
                Bucket=BUCKET_RAW,
                Key=key,
                Body=content,
                Metadata={
                    "report_id": str(report_id) if report_id is not None else "",
                },
            )
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'BRONZE_OK' WHERE file_id = %s",
                (file_id,)
            )
            conn.commit()
            print(f"[OK]   file_id={file_id}  ({fmt})")
            ok_count += 1

        except Exception as e:
            err_msg = str(e)
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s WHERE file_id = %s",
                (err_msg, file_id)
            )
            cur.execute("""
                INSERT INTO etl_logs_dados (file_id, step, error_message)
                VALUES (%s, %s, %s)
            """, (file_id, "ingest_raw", err_msg))
            conn.commit()
            print(f"[ERRO] file_id={file_id}  {e}")
            err_count += 1

    cur.close(); conn.close()
    print(f"ingest_raw concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    main()
