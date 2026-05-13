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


def _log_error(file_id, step: str, message: str):
    """Regista um erro em etl_logs_dados com conexão própria em autocommit.

    Usar uma conexão separada garante que o log é persistido mesmo que a
    transação principal faça rollback ou esteja em estado de erro.
    """
    try:
        _conn = psycopg2.connect(**DB_CONFIG)
        _conn.autocommit = True
        _cur = _conn.cursor()
        _cur.execute(
            "INSERT INTO etl_logs_dados (file_id, step, error_message) "
            "VALUES (%s, %s, %s)",
            (str(file_id) if file_id is not None else None, step, message),
        )
        _cur.close()
        _conn.close()
    except Exception as log_exc:
        print(f"[AVISO] Não foi possível registar erro no etl_logs: {log_exc}")


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

    # Conjunto de file_ids que deveriam chegar ao Silver nesta execução:
    # novos ficheiros em op_data (após last_run) sem erros nas fases anteriores.
    if last_run:
        cur.execute("""
            SELECT d.file_id FROM op_data d
            WHERE d.extract_function IS NOT NULL AND d.extract_function != ''
              AND d.created_at > %s
              AND NOT EXISTS (
                  SELECT 1 FROM etl_logs_dados l
                  WHERE l.file_id = d.file_id::text
                    AND l.step IN ('validate_opdata', 'ingest_raw', 'validate_bronze')
              )
        """, (last_run,))
    else:
        cur.execute("""
            SELECT d.file_id FROM op_data d
            WHERE d.extract_function IS NOT NULL AND d.extract_function != ''
              AND NOT EXISTS (
                  SELECT 1 FROM etl_logs_dados l
                  WHERE l.file_id = d.file_id::text
                    AND l.step IN ('validate_opdata', 'ingest_raw', 'validate_bronze')
              )
        """)
    expected_ids = {str(r[0]) for r in cur.fetchall()}

    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=BUCKET_RAW)

    ok_count         = 0
    err_count        = 0
    found_in_bronze  = set()   # todos os keys vistos no bucket Bronze
    processed_ids    = set()   # keys para os quais a transformação foi tentada

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            found_in_bronze.add(key)

            try:
                head     = s3.head_object(Bucket=BUCKET_RAW, Key=key)
                metadata = head.get("Metadata", {})
            except Exception as e:
                print(f"[ERRO] Metadata de {key}: {e}")
                _log_error(key, "transform", f"Não foi possível ler metadata do Bronze: {e}")
                err_count += 1
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
                print(f"[ERRO] Sem extract_function: {key}")
                _log_error(key, "transform", "Sem extract_function na metadata Bronze")
                err_count += 1
                continue

            if extract_function not in EXTRACT_FUNCTIONS:
                print(f"[ERRO] Função desconhecida '{extract_function}': {key}")
                _log_error(key, "transform", f"Função desconhecida: {extract_function}")
                err_count += 1
                continue

            print(f"A transformar: {key} ({extract_function})")
            processed_ids.add(key)

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
                        "created_at": metadata.get("created_at", ""),
                    },
                )
                print(f"[OK]   {key} -> {key}.parquet")
                ok_count += 1

            except Exception as e:
                print(f"[ERRO] {key}: {e}")
                _log_error(key, "transform", str(e))
                err_count += 1

    # Detetar ficheiros esperados que não estavam no bucket Bronze de todo
    missing_from_bronze = expected_ids - found_in_bronze
    for missing_key in sorted(missing_from_bronze):
        print(f"[ERRO] file_id={missing_key} esperado mas não encontrado no bucket Bronze")
        _log_error(missing_key, "transform",
                   "Ficheiro esperado não encontrado no bucket Bronze")
        err_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"transform concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    transformar()
