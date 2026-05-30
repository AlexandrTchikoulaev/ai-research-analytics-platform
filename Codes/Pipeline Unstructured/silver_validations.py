"""
Valida os PDFs no bucket bronze-unstructured correspondentes a relatórios BRONZE_OK.
Remove objectos com conteúdo inválido, actualiza pipeline_status e regista erros em etl_logs_pdfs.
Erros de acesso (rede, S3) marcam FAILED mas não apagam o ficheiro.
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
    try:
        cur = conn.cursor()
        s3  = boto3.client("s3", **MINIO_CONFIG)

        cur.execute("""
            SELECT report_id, file_name
            FROM op_report
            WHERE pipeline_status = 'BRONZE_OK'
            ORDER BY report_id
            FOR UPDATE SKIP LOCKED
        """)
        rows = cur.fetchall()

        invalid_records = []
        ok_count = 0

        for report_id, file_name in rows:
            errors = []
            content_invalid = False

            if file_name is None:
                errors.append("file_name é NULL")
            else:
                try:
                    response = s3.get_object(
                        Bucket=BUCKET_UNSTRUCTURED,
                        Key=file_name,
                        Range="bytes=0-1023",
                    )
                    try:
                        header = response["Body"].read()
                    finally:
                        response["Body"].close()
                    if b"%PDF" not in header:
                        errors.append(f"Conteúdo não é PDF válido (primeiros bytes: {header[:16]!r})")
                        content_invalid = True
                except Exception as e:
                    errors.append(f"Não foi possível ler o PDF do bucket: {e}")
                    # Não apagar — pode ser erro de rede transitório

            if errors:
                msg = "; ".join(errors)
                print(f"[INVÁLIDO] report_id={report_id}  {file_name}: {msg}")
                invalid_records.append((report_id, file_name, msg, content_invalid))
            else:
                ok_count += 1

        # Actualizar BD numa única transacção
        for report_id, file_name, msg, _ in invalid_records:
            cur.execute(
                "UPDATE op_report SET pipeline_status = 'FAILED', pipeline_error = %s WHERE report_id = %s",
                (msg, report_id)
            )
            cur.execute(
                "INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                (report_id, file_name, "validate_bronze_unstructured", msg),
            )
        conn.commit()

        # Apagar do bucket apenas ficheiros com conteúdo inválido confirmado — depois do commit
        for report_id, file_name, msg, content_invalid in invalid_records:
            if content_invalid and file_name is not None:
                try:
                    s3.delete_object(Bucket=BUCKET_UNSTRUCTURED, Key=file_name)
                except Exception as del_e:
                    print(f"  Erro ao apagar {file_name}: {del_e}")

        cur.close()
        print(f"validate_bronze_unstructured — {ok_count} válidos, {len(invalid_records)} inválidos/removidos")
    finally:
        conn.close()


if __name__ == "__main__":
    validate()
