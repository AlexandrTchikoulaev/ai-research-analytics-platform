import psycopg2
import requests
import boto3

DB_PIPELINE = {
    "host": "localhost",
    "port": 5433,
    "dbname": "pipeline_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

DB_OPERATIONAL = {
    "host": "localhost",
    "port": 5433,
    "dbname": "operational_db",
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


def main():
    print("A correr ingest_unstructured...")

    conn_pipe = psycopg2.connect(**DB_PIPELINE)
    cur_pipe  = conn_pipe.cursor()
    conn_op   = psycopg2.connect(**DB_OPERATIONAL)
    cur_op    = conn_op.cursor()
    s3 = boto3.client("s3", **MINIO_CONFIG)

    # Garantir que o bucket existe
    try:
        s3.head_bucket(Bucket=BUCKET_UNSTRUCTURED)
    except Exception:
        s3.create_bucket(Bucket=BUCKET_UNSTRUCTURED)

    # Obter last_run do processo de PDFs
    cur_pipe.execute("SELECT last_run FROM etl_data WHERE process_name = %s", (PROCESS_NAME,))
    row = cur_pipe.fetchone()
    last_run = row[0] if row else None

    # Buscar novos relatórios desde last_run
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
        cur_pipe.close(); conn_pipe.close()
        cur_op.close();   conn_op.close()
        return

    session = requests.Session()
    ok_count = 0
    err_count = 0

    for report_id, file_name, report_url in rows:
        try:
            print(f"A descarregar: {report_url}")
            response = session.get(report_url, timeout=60)
            response.raise_for_status()
            content = response.content

            if not is_valid_pdf(content):
                raise ValueError(f"Conteúdo não é um PDF válido (não começa com %PDF)")

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
            cur_pipe.execute("""
                INSERT INTO etl_logs_pdfs (file_name, step, status, error_message)
                VALUES (%s, %s, %s, %s)
            """, (file_name, "ingest_unstructured", "error", str(e)))
            print(f"[ERRO] {file_name}: {e}")
            err_count += 1

    conn_pipe.commit()
    cur_pipe.close(); conn_pipe.close()
    cur_op.close();   conn_op.close()
    print(f"ingest_unstructured concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    main()
