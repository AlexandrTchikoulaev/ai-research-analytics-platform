"""
Valida os objetos Bronze correspondentes a ficheiros com pipeline_status = 'BRONZE_OK'.
Remove objetos inválidos, actualiza pipeline_status e regista erros em etl_logs_dados.
"""
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


def validate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    s3 = boto3.client("s3", **MINIO_CONFIG)

    cur.execute("""
        SELECT file_id, report_id
        FROM op_data
        WHERE pipeline_status = 'BRONZE_OK'
    """)
    rows = cur.fetchall()

    ok_count = 0
    err_count = 0

    for file_id, db_report_id in rows:
        key = str(file_id)
        errors = []

        try:
            head = s3.head_object(Bucket=BUCKET_RAW, Key=key)
            metadata = head.get("Metadata", {})
        except Exception as e:
            errors.append(f"Objecto Bronze não encontrado ou inacessível: {e}")
            metadata = {}

        if not errors:
            report_id_str = metadata.get("report_id", "")

            try:
                report_id = int(report_id_str) if report_id_str else None
            except (ValueError, TypeError):
                errors.append(f"report_id inválido na metadata: '{report_id_str}'")
                report_id = None

            if report_id is not None and db_report_id != report_id:
                errors.append(
                    f"report_id inconsistente: metadata={report_id}, db={db_report_id}"
                )

        if errors:
            msg = "; ".join(errors)
            print(f"[INVÁLIDO] file_id={file_id}: {msg}")
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s WHERE file_id = %s",
                (msg, file_id)
            )
            cur.execute("""
                INSERT INTO etl_logs_dados (file_id, step, error_message)
                VALUES (%s, %s, %s)
            """, (file_id, "validate_bronze", msg))
            conn.commit()
            try:
                s3.delete_object(Bucket=BUCKET_RAW, Key=key)
            except Exception as del_e:
                print(f"  Erro ao apagar {key}: {del_e}")
            err_count += 1
        else:
            ok_count += 1

    cur.close()
    conn.close()
    print(f"validate_bronze — {ok_count} válidos, {err_count} inválidos/removidos")


if __name__ == "__main__":
    validate()
