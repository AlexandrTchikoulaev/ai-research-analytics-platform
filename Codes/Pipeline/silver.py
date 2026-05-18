import json
import io
import os
import pandas as pd
import boto3
import psycopg2

from silver_functions import EXTRACT_FUNCTIONS, clean_dataframe

DB_CONFIG = {
    "host": "localhost", "port": 5433, "dbname": "gestao_db",
    "user": "projeto_utilizador", "password": "projeto",
}

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

BUCKET_RAW    = "bronze"
BUCKET_SILVER = "silver"


_FALLBACK_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "silver_errors_fallback.log")

def _log_error(cur, conn, file_id, step: str, message: str):
    """Regista um erro em etl_logs_dados na conexão principal."""
    fid_str = str(file_id) if file_id is not None else None
    try:
        cur.execute(
            "INSERT INTO etl_logs_dados (file_id, step, error_message) VALUES (%s, %s, %s)",
            (fid_str, step, message),
        )
    except Exception as log_exc:
        print(f"[ERRO-LOG] file_id={fid_str} step={step}: {message}")
        print(f"[ERRO-LOG] Falha ao registar em etl_logs: {log_exc}")
        try:
            from datetime import datetime as _dt
            with open(_FALLBACK_LOG, "a", encoding="utf-8") as _f:
                _f.write(f"{_dt.now().isoformat()} | file_id={fid_str} step={step}: {message}\n")
                _f.write(f"  DB error: {log_exc}\n")
        except Exception:
            pass


def detect_format(content: bytes) -> str:
    snippet = content[:20].lstrip()
    if snippet.startswith(b"PK"):
        return "excel"
    if snippet.startswith(b"\xd0\xcf"):
        return "excel"
    if snippet.startswith(b"<?xml") or snippet.startswith(b"<"):
        return "xml"
    if snippet.startswith(b"{") or snippet.startswith(b"["):
        return "json"
    return "csv"


def read_raw_object(s3, key: str):
    response = s3.get_object(Bucket=BUCKET_RAW, Key=key)
    content  = response["Body"].read()
    fmt = detect_format(content)
    if fmt == "json":
        return json.loads(content)
    elif fmt == "csv":
        return pd.read_csv(io.BytesIO(content))
    elif fmt == "excel":
        return pd.read_excel(io.BytesIO(content))
    else:
        return json.loads(content)


def transformar():
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    s3   = boto3.client("s3", **MINIO_CONFIG)

    try:
        s3.head_bucket(Bucket=BUCKET_SILVER)
    except Exception:
        s3.create_bucket(Bucket=BUCKET_SILVER)

    cur.execute("""
        SELECT file_id, extract_function, report_id
        FROM op_data
        WHERE pipeline_status = 'BRONZE_OK'
        ORDER BY file_id
        FOR UPDATE SKIP LOCKED
    """)
    rows = cur.fetchall()

    if not rows:
        print("Sem ficheiros BRONZE_OK para transformar.")
        cur.close(); conn.close()
        return

    file_ids = [r[0] for r in rows]
    cur.execute(
        "UPDATE op_data SET pipeline_status = 'PROCESSING' WHERE file_id = ANY(%s)",
        (file_ids,)
    )
    conn.commit()

    ok_count  = 0
    err_count = 0

    for file_id, extract_function, report_id in rows:
        key = str(file_id)

        if not extract_function or extract_function not in EXTRACT_FUNCTIONS:
            err_msg = f"extract_function inválida ou desconhecida: '{extract_function}'"
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s WHERE file_id = %s",
                (err_msg, file_id)
            )
            _log_error(cur, conn, file_id, "transform", err_msg)
            conn.commit()
            print(f"[ERRO] file_id={file_id}: {err_msg}")
            err_count += 1
            continue

        print(f"A transformar: {key} ({extract_function})")

        try:
            data = read_raw_object(s3, key)
            df   = EXTRACT_FUNCTIONS[extract_function](data)

            if df is None or df.empty:
                raise ValueError("DataFrame vazio após transformação")

            df = clean_dataframe(df)

            if df is None or df.empty:
                raise ValueError("DataFrame vazio após clean_dataframe")

            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            parquet_bytes = buffer.getvalue()

            if not parquet_bytes:
                raise ValueError("Parquet gerado está vazio (0 bytes)")

            s3.put_object(
                Bucket=BUCKET_SILVER,
                Key=f"{key}.parquet",
                Body=parquet_bytes,
                Metadata={
                    "report_id": str(report_id) if report_id is not None else "",
                },
            )

            cur.execute(
                "UPDATE op_data SET pipeline_status = 'SILVER_OK' WHERE file_id = %s",
                (file_id,)
            )
            conn.commit()
            print(f"[OK]   {key} -> {key}.parquet")
            ok_count += 1

        except Exception as e:
            err_msg = str(e)
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s WHERE file_id = %s",
                (err_msg, file_id)
            )
            _log_error(cur, conn, file_id, "transform", err_msg)
            conn.commit()
            print(f"[ERRO] {key}: {e}")
            err_count += 1

    cur.close()
    conn.close()
    print(f"transform concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    transformar()
