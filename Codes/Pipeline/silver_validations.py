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


def _log_error_bronze(file_id, message: str) -> bool:
    """Escreve um erro de validate_bronze em conexão autocommit independente.

    Retorna True se o log foi persistido, False caso contrário.
    Só apagar o objeto Bronze após confirmação do log.
    """
    try:
        _conn = psycopg2.connect(**DB_PIPELINE)
        _conn.autocommit = True
        _cur = _conn.cursor()
        _cur.execute(
            "INSERT INTO etl_logs_dados (file_id, step, error_message) "
            "VALUES (%s, %s, %s)",
            (str(file_id) if file_id is not None else None, "validate_bronze", message),
        )
        _cur.close()
        _conn.close()
        return True
    except Exception as log_exc:
        print(f"[AVISO] Não foi possível registar erro validate_bronze: {log_exc}")
        return False


def validate():
    conn_op = psycopg2.connect(**DB_OPERATIONAL)
    cur_op  = conn_op.cursor()
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
                _log_error_bronze(key, f"Não foi possível ler metadata: {e}")
                err_count += 1
                continue

            report_id_str = metadata.get("report_id", "")
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
                    SELECT report_id, extract_function
                    FROM op_data WHERE file_id = %s
                """, (file_id,))
                row = cur_op.fetchone()
                if not row:
                    errors.append(f"file_id={file_id} não existe em op_data")
                else:
                    db_report_id, db_extract_fn = row
                    if report_id is not None and db_report_id != report_id:
                        errors.append(
                            f"report_id inconsistente: metadata={report_id}, db={db_report_id}"
                        )
                    if extract_function and db_extract_fn and extract_function != db_extract_fn:
                        errors.append(
                            f"extract_function inconsistente: metadata={extract_function}, db={db_extract_fn}"
                        )

            if errors:
                msg = "; ".join(errors)
                print(f"[INVÁLIDO] {key}: {msg}")
                logged = _log_error_bronze(file_id, msg)
                if logged:
                    try:
                        s3.delete_object(Bucket=BUCKET_RAW, Key=key)
                    except Exception as del_e:
                        print(f"  Erro ao apagar {key}: {del_e}")
                else:
                    print(f"  [AVISO] {key} não apagado do Bronze porque o log falhou")
                err_count += 1
            else:
                ok_count += 1

    cur_op.close()
    conn_op.close()
    print(f"validate_bronze — {ok_count} válidos, {err_count} inválidos/removidos")


if __name__ == "__main__":
    validate()
