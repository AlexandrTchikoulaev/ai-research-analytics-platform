"""
Valida os ficheiros Parquet no bucket Silver correspondentes a ficheiros
com pipeline_status = 'SILVER_OK'.
Remove ficheiros inválidos, actualiza pipeline_status e regista erros em etl_logs_dados.
"""
import io
import psycopg2
import boto3
import pandas as pd

DB_CONFIG = {
    "host": "localhost", "port": 5433, "dbname": "gestao_db",
    "user": "projeto_utilizador", "password": "projeto",
}

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

BUCKET_SILVER = "silver"

_REQUIRED_COLS = {"location_code", "indicator_code", "indicator_name", "year", "value"}


def validate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    s3 = boto3.client("s3", **MINIO_CONFIG)

    cur.execute("SELECT file_id FROM op_data WHERE pipeline_status = 'SILVER_OK'")
    rows = cur.fetchall()

    ok_count = 0
    err_count = 0

    for (file_id,) in rows:
        key = f"{file_id}.parquet"
        errors = []

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
                missing = _REQUIRED_COLS - set(df.columns)
                if missing:
                    errors.append(f"Colunas em falta: {sorted(missing)}")

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
            """, (file_id, "validate_silver", msg))
            conn.commit()
            try:
                s3.delete_object(Bucket=BUCKET_SILVER, Key=key)
            except Exception as del_e:
                print(f"  Erro ao apagar {key}: {del_e}")
            err_count += 1
        else:
            ok_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"validate_silver — {ok_count} válidos, {err_count} inválidos/removidos")


if __name__ == "__main__":
    validate()
