import json
import io
import pandas as pd
import boto3
import psycopg2
from dateutil.parser import parse as parse_date
from datetime import timezone

from silver_functions import EXTRACT_FUNCTIONS, clean_dataframe

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

BUCKET_RAW    = "bronze"
BUCKET_SILVER = "silver"
PROCESS_NAME  = "etl_dados"


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

    cur.execute("SELECT last_run FROM etl_data WHERE process_name = %s", (PROCESS_NAME,))
    row      = cur.fetchone()
    last_run = row[0] if row else None

    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=BUCKET_RAW)

    ok_count  = 0
    err_count = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            try:
                head     = s3.head_object(Bucket=BUCKET_RAW, Key=key)
                metadata = head.get("Metadata", {})
            except Exception as e:
                print(f"[ERRO] Metadata de {key}: {e}")
                continue

            extract_function = metadata.get("extract_function", "")
            created_at_str   = metadata.get("created_at", "")

            # Filtro incremental por created_at da metadata
            if last_run and created_at_str:
                try:
                    created_dt = parse_date(created_at_str)
                    if created_dt.tzinfo is None:
                        created_dt = created_dt.replace(tzinfo=timezone.utc)
                    lr = last_run if last_run.tzinfo else last_run.replace(tzinfo=timezone.utc)
                    if created_dt <= lr:
                        continue
                except Exception:
                    pass

            if not extract_function:
                print(f"[SKIP] Sem extract_function: {key}")
                continue

            if extract_function not in EXTRACT_FUNCTIONS:
                print(f"[SKIP] Função desconhecida '{extract_function}': {key}")
                cur.execute("""
                    INSERT INTO etl_logs_dados (file_id, step, status, error_message)
                    VALUES (%s, %s, %s, %s)
                """, (key, "transform", "error",
                      f"Função desconhecida: {extract_function}"))
                err_count += 1
                continue

            print(f"A transformar: {key} ({extract_function})")

            try:
                data  = read_raw_object(s3, key)
                df    = EXTRACT_FUNCTIONS[extract_function](data)

                if df is None or df.empty:
                    raise ValueError("DataFrame vazio após transformação")

                df = clean_dataframe(df)

                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)

                s3.put_object(
                    Bucket=BUCKET_SILVER,
                    Key=f"{key}.parquet",
                    Body=buffer.getvalue(),
                    Metadata={
                        "report_id":  metadata.get("report_id", ""),
                        "file_type":  metadata.get("file_type", ""),
                        "created_at": metadata.get("created_at", ""),
                    },
                )
                print(f"[OK]   {key} -> {key}.parquet")
                ok_count += 1

            except Exception as e:
                cur.execute("""
                    INSERT INTO etl_logs_dados (file_id, step, status, error_message)
                    VALUES (%s, %s, %s, %s)
                """, (key, "transform", "error", str(e)))
                print(f"[ERRO] {key}: {e}")
                err_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"transform concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    transformar()
