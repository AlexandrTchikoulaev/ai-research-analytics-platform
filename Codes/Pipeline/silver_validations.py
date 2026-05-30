"""
Valida os objetos Bronze correspondentes a ficheiros com pipeline_status = 'BRONZE_OK'.
Actualiza pipeline_status e regista erros em etl_logs_dados.
"""
import psycopg2
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import DB_CONFIG, MINIO_CONFIG, BUCKET_RAW

MAX_WORKERS = 8


def _validate_one(file_id: int, db_report_id) -> bool:
    """Valida um objecto Bronze num thread dedicado."""
    s3 = boto3.client("s3", **MINIO_CONFIG)
    key = str(file_id)
    errors = []

    try:
        head = s3.head_object(Bucket=BUCKET_RAW, Key=key)
        metadata = head.get("Metadata", {})
        if head.get("ContentLength", 0) == 0:
            errors.append("Ficheiro Bronze está vazio (0 bytes)")
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
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s WHERE file_id = %s",
                (msg, file_id)
            )
            cur.execute(
                "INSERT INTO etl_logs_dados (file_id, step, error_message) VALUES (%s, %s, %s)",
                (file_id, "validate_bronze", msg)
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        return False

    return True


def validate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT file_id, report_id
        FROM op_data
        WHERE pipeline_status = 'BRONZE_OK'
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    ok_count = 0
    err_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_validate_one, file_id, db_report_id): file_id
            for file_id, db_report_id in rows
        }
        for future in as_completed(futures):
            try:
                if future.result():
                    ok_count += 1
                else:
                    err_count += 1
            except Exception as e:
                file_id = futures[future]
                print(f"[ERRO] file_id={file_id} exceção não tratada: {e}")
                err_count += 1

    print(f"validate_bronze — {ok_count} válidos, {err_count} inválidos")


if __name__ == "__main__":
    validate()
