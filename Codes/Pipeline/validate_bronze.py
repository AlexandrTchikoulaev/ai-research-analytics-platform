"""
Valida os objetos presentes no bucket Bronze (raw).
Verifica consistência dos metadados com op_data.
Remove objetos inválidos e regista erros em etl_logs_dados.
"""
import psycopg2
import boto3

DB_OPERATIONAL = {
    "host": "localhost",
    "port": 5433,
    "dbname": "gestao_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

DB_PIPELINE = {
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


def validate():
    conn_op   = psycopg2.connect(**DB_OPERATIONAL)
    cur_op    = conn_op.cursor()
    conn_pipe = psycopg2.connect(**DB_PIPELINE)
    cur_pipe  = conn_pipe.cursor()
    s3 = boto3.client("s3", **MINIO_CONFIG)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_RAW)

    ok_count = 0
    err_count = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            try:
                head = s3.head_object(Bucket=BUCKET_RAW, Key=key)
                metadata = head.get("Metadata", {})
            except Exception as e:
                print(f"[ERRO] Não foi possível ler metadata de {key}: {e}")
                err_count += 1
                continue

            report_id_str = metadata.get("report_id", "")
            file_type = metadata.get("file_type", "")
            extract_function = metadata.get("extract_function", "")
            errors = []

            # file_id é a própria chave do objeto
            try:
                file_id = int(key)
            except (ValueError, TypeError):
                errors.append(f"chave inválida (não é um file_id): '{key}'")
                file_id = None

            # Validar report_id
            try:
                report_id = int(report_id_str) if report_id_str else None
            except (ValueError, TypeError):
                errors.append(f"report_id inválido na metadata: '{report_id_str}'")
                report_id = None

            # Verificar consistência com op_data
            if file_id is not None:
                cur_op.execute("""
                    SELECT report_id, extract_function, file_type
                    FROM op_data WHERE file_id = %s
                """, (file_id,))
                row = cur_op.fetchone()
                if not row:
                    errors.append(f"file_id={file_id} não existe em op_data")
                else:
                    db_report_id, db_extract_fn, db_file_type = row
                    if report_id is not None and db_report_id != report_id:
                        errors.append(
                            f"report_id inconsistente: metadata={report_id}, db={db_report_id}"
                        )
                    if extract_function and db_extract_fn and extract_function != db_extract_fn:
                        errors.append(
                            f"extract_function inconsistente: metadata={extract_function}, db={db_extract_fn}"
                        )
                    if file_type and db_file_type and file_type != db_file_type:
                        errors.append(
                            f"file_type inconsistente: metadata={file_type}, db={db_file_type}"
                        )

            if errors:
                msg = "; ".join(errors)
                print(f"[INVÁLIDO] {key}: {msg}")
                # Remover do bucket Bronze
                try:
                    s3.delete_object(Bucket=BUCKET_RAW, Key=key)
                except Exception as del_e:
                    print(f"  Erro ao apagar {key}: {del_e}")

                cur_pipe.execute("""
                    INSERT INTO etl_logs_dados (file_id, step, status, error_message)
                    VALUES (%s, %s, %s, %s)
                """, (file_id, "validate_bronze", "error", msg))
                err_count += 1
            else:
                ok_count += 1

    conn_pipe.commit()
    cur_pipe.close(); conn_pipe.close()
    cur_op.close();   conn_op.close()
    print(f"validate_bronze — {ok_count} válidos, {err_count} inválidos/removidos")


if __name__ == "__main__":
    validate()
