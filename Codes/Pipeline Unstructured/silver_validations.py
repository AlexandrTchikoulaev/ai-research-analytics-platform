"""
Valida os PDFs no bucket bronze-unstructured correspondentes a relatórios BRONZE_OK.
Remove objectos inválidos, actualiza pipeline_status e regista erros em etl_logs_pdfs.
"""
import psycopg2
import boto3

DB_CONFIG = {
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


def validate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    s3   = boto3.client("s3", **MINIO_CONFIG)

    cur.execute("""
        SELECT report_id, file_name
        FROM op_report
        WHERE pipeline_status = 'BRONZE_OK'
    """)
    rows = cur.fetchall()

    ok_count  = 0
    err_count = 0

    for report_id, file_name in rows:
        errors = []

        try:
            response = s3.get_object(
                Bucket=BUCKET_UNSTRUCTURED,
                Key=file_name,
                Range="bytes=0-3",
            )
            header = response["Body"].read()
            if header != b"%PDF":
                errors.append(f"Conteúdo não é um PDF válido (header: {header})")
        except Exception as e:
            errors.append(f"Não foi possível ler o PDF: {e}")

        if errors:
            msg = "; ".join(errors)
            print(f"[INVÁLIDO] report_id={report_id}  {file_name}: {msg}")
            cur.execute(
                "UPDATE op_report SET pipeline_status = 'FAILED', pipeline_error = %s WHERE report_id = %s",
                (msg, report_id)
            )
            cur.execute(
                "INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                (report_id, file_name, "validate_bronze_unstructured", msg),
            )
            conn.commit()
            try:
                s3.delete_object(Bucket=BUCKET_UNSTRUCTURED, Key=file_name)
            except Exception as del_e:
                print(f"  Erro ao apagar {file_name}: {del_e}")
            err_count += 1
        else:
            ok_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"validate_bronze_unstructured — {ok_count} válidos, {err_count} inválidos/removidos")


if __name__ == "__main__":
    validate()
