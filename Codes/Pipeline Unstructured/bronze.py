import psycopg2
import requests
import boto3

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

BUCKET_UNSTRUCTURED = "bronze-unstructured"
PROCESS_NAME = "etl_pdfs"


def is_valid_pdf(content: bytes) -> bool:
    return content[:4] == b"%PDF"


def main(valid_ids=None):
    """
    valid_ids: list of report_id integers pre-validated by validate_op_report.
    When None (standalone run), falls back to last_run timestamp filtering.
    """
    print("A correr bronze (ingest unstructured)...")

    conn_pipe = conn_op = None
    try:
        conn_pipe = psycopg2.connect(**DB_PIPELINE)
        cur_pipe  = conn_pipe.cursor()
        conn_op   = psycopg2.connect(**DB_OPERATIONAL)
        cur_op    = conn_op.cursor()
        s3 = boto3.client("s3", **MINIO_CONFIG)

        try:
            s3.head_bucket(Bucket=BUCKET_UNSTRUCTURED)
        except Exception:
            s3.create_bucket(Bucket=BUCKET_UNSTRUCTURED)

        if valid_ids is not None:
            if not valid_ids:
                print("Sem PDFs válidos para ingestão.")
                return
            cur_op.execute("""
                SELECT report_id, file_name, report_url
                FROM op_report
                WHERE report_id = ANY(%s) AND report_url IS NOT NULL AND report_url != ''
            """, (valid_ids,))
        else:
            cur_pipe.execute("SELECT last_run FROM etl_data WHERE process_name = %s", (PROCESS_NAME,))
            row = cur_pipe.fetchone()
            last_run = row[0] if row else None
            if last_run:
                cur_op.execute("""
                    SELECT report_id, file_name, report_url
                    FROM op_report
                    WHERE created_at > %s AND report_url IS NOT NULL AND report_url != ''
                """, (last_run,))
            else:
                cur_op.execute("""
                    SELECT report_id, file_name, report_url
                    FROM op_report
                    WHERE report_url IS NOT NULL AND report_url != ''
                """)

        rows = cur_op.fetchall()

        if not rows:
            print("Sem novos PDFs para ingestão.")
            return

        # Pre-fetch existing bucket keys to avoid re-downloading
        existing_keys = set()
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET_UNSTRUCTURED):
            for obj in page.get("Contents", []):
                existing_keys.add(obj["Key"])

        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/pdf,*/*",
        })
        ok_count = 0
        err_count = 0

        for report_id, file_name, report_url in rows:
            if file_name in existing_keys:
                print(f"[SKIP] Já existe no bucket: {file_name}")
                ok_count += 1
                continue

            if report_url is None:
                # Ficheiro devia ter sido uploaded directamente mas não está no bucket
                cur_pipe.execute(
                    "INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                    (report_id, file_name, "bronze", "report_url é NULL e ficheiro não encontrado no bucket — faça upload directo via /op_report/upload"),
                )
                print(f"[ERRO] report_id={report_id}: sem report_url e sem ficheiro no bucket")
                err_count += 1
                continue

            try:
                print(f"A descarregar: {report_url}")
                response = session.get(report_url, timeout=60, allow_redirects=True)
                response.raise_for_status()
                content = response.content

                if not is_valid_pdf(content):
                    ct = response.headers.get("Content-Type", "desconhecido")
                    if "html" in ct.lower() or content[:1] == b"<":
                        raise ValueError(
                            f"O servidor devolveu uma página HTML em vez do PDF "
                            f"(possivelmente bloqueio/CAPTCHA). Content-Type: {ct}"
                        )
                    raise ValueError(f"Conteúdo não é um PDF válido (não começa com %PDF). Content-Type: {ct}")

                s3.put_object(
                    Bucket=BUCKET_UNSTRUCTURED,
                    Key=file_name,
                    Body=content,
                    ContentType="application/pdf",
                    Metadata={
                        "report_id": str(report_id),
                        "file_name": file_name,
                    },
                )
                print(f"[OK]   {file_name}")
                ok_count += 1

            except Exception as e:
                cur_pipe.execute(
                    "INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                    (report_id, file_name, "bronze", str(e)),
                )
                print(f"[ERRO] {file_name}: {e}")
                err_count += 1

        conn_pipe.commit()
        print(f"bronze concluído — {ok_count} OK, {err_count} erros")

    except Exception as e:
        if conn_pipe:
            try:
                conn_pipe.cursor().execute(
                    "INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                    (None, "N/A", "bronze", str(e)),
                )
                conn_pipe.commit()
            except Exception:
                pass
        raise
    finally:
        if conn_pipe:
            conn_pipe.close()
        if conn_op:
            conn_op.close()


if __name__ == "__main__":
    main()
