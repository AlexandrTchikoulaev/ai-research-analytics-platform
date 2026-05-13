"""
Valida os ficheiros Parquet presentes no bucket Silver (transformed).
Verifica estrutura e colunas obrigatórias.
Remove ficheiros inválidos e regista erros em etl_logs_dados.
"""
import io
import psycopg2
import boto3
import pandas as pd

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

BUCKET_SILVER = "silver"

_INDICATOR_COLS = {"code", "name"}
_VALUE_COLS = {"location_code", "indicator_code", "year", "value", "value_type"}


def validate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    s3 = boto3.client("s3", **MINIO_CONFIG)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_SILVER)

    ok_count = 0
    err_count = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            errors = []
            file_id = None
            fname = key

            try:
                head = s3.head_object(Bucket=BUCKET_SILVER, Key=key)
                metadata = head.get("Metadata", {})
            except Exception as e:
                print(f"[ERRO] Metadata de {key}: {e}")
                err_count += 1
                continue

            try:
                file_id = int(key.rsplit(".", 1)[0])
            except (ValueError, TypeError):
                errors.append(f"chave inválida (não é um file_id): '{key}'")

            # Ler o ficheiro Parquet
            try:
                response = s3.get_object(Bucket=BUCKET_SILVER, Key=key)
                buf = io.BytesIO(response["Body"].read())
                df = pd.read_parquet(buf, engine="pyarrow")
            except Exception as e:
                errors.append(f"Erro ao ler Parquet: {e}")
                df = None

            if df is not None:
                if df.empty:
                    errors.append("Ficheiro Parquet está vazio")
                else:
                    cols = set(df.columns)
                    if cols == _INDICATOR_COLS:
                        pass
                    elif _VALUE_COLS <= cols:
                        pass
                    else:
                        errors.append(f"Estrutura de colunas não reconhecida: {sorted(cols)}")

            if errors:
                msg = "; ".join(errors)
                print(f"[INVÁLIDO] {key}: {msg}")
                try:
                    s3.delete_object(Bucket=BUCKET_SILVER, Key=key)
                except Exception as del_e:
                    print(f"  Erro ao apagar {key}: {del_e}")

                cur.execute("""
                    INSERT INTO etl_logs_dados (file_id, step, error_message)
                    VALUES (%s, %s, %s)
                """, (file_id, "validate_silver", msg))
                err_count += 1
            else:
                ok_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"validate_silver — {ok_count} válidos, {err_count} inválidos/removidos")


if __name__ == "__main__":
    validate()
