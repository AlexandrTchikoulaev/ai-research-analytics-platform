import json
import io
import os
import pandas as pd
import boto3
import psycopg2

from silver_functions import EXTRACT_FUNCTIONS, clean_dataframe
from silver_function_generator import generate_and_validate, save_to_auto_store

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
        return io.BytesIO(content)
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
        SELECT d.file_id, d.report_id, r.source_code, d.auto_generate
        FROM op_data d
        JOIN op_report r ON r.report_id = d.report_id
        WHERE d.pipeline_status = 'BRONZE_OK'
        ORDER BY d.file_id
        FOR UPDATE OF d SKIP LOCKED
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

    # Carregar todos os mapeamentos uma vez
    cur.execute("SELECT source_code, extract_function, ai_extract_function, generation_hint FROM source_function_mapping")
    manual_mapping = {}
    ai_mapping     = {}
    hint_mapping   = {}
    for r in cur.fetchall():
        if r[1]: manual_mapping[r[0]] = r[1]
        if r[2]: ai_mapping[r[0]]     = r[2]
        if r[3]: hint_mapping[r[0]]   = r[3]

    ok_count  = 0
    err_count = 0

    for file_id, report_id, source_code, auto_generate in rows:
        key = str(file_id)

        extract_function = None
        fn_source = None  # 'ai_gerada' | 'ai_cache' | 'manual'

        if not auto_generate:
            # Modo manual: ignora AI
            extract_function = manual_mapping.get(source_code)
            fn_source = "manual"
            if not extract_function:
                err_msg = (
                    f"Sem mapeamento manual para '{source_code}'. "
                    f"Configure um mapeamento no Painel de Controlo."
                )
                cur.execute(
                    "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s WHERE file_id = %s",
                    (err_msg, file_id)
                )
                _log_error(cur, conn, file_id, "transform", err_msg)
                conn.commit()
                print(f"[ERRO] file_id={file_id}: {err_msg}")
                err_count += 1
                continue
        else:
            # Modo AI: usar mapeamento AI em cache → gerar → cair no manual
            extract_function = ai_mapping.get(source_code)
            if extract_function:
                fn_source = "ai_cache"

            if not extract_function:
                print(f"[AI] Sem mapeamento AI para '{source_code}', a gerar função para file_id={file_id}...")
                try:
                    content = s3.get_object(Bucket=BUCKET_RAW, Key=key)["Body"].read()
                    hint = hint_mapping.get(source_code)
                    gen = generate_and_validate(content, hint=hint)
                except Exception as e:
                    gen = {"valid": False, "error": str(e), "function_name": None, "code": None}

                if gen["valid"]:
                    fn_name = gen["function_name"]
                    save_to_auto_store(fn_name, gen["code"])
                    ns = {"pd": pd}
                    exec(compile(gen["code"], "<auto>", "exec"), ns)
                    EXTRACT_FUNCTIONS[fn_name] = ns[fn_name]
                    cur.execute("""
                        INSERT INTO source_function_mapping (source_code, ai_extract_function)
                        VALUES (%s, %s)
                        ON CONFLICT (source_code) DO UPDATE SET ai_extract_function = EXCLUDED.ai_extract_function
                    """, (source_code, fn_name))
                    conn.commit()
                    ai_mapping[source_code] = fn_name
                    extract_function = fn_name
                    fn_source = "ai_gerada"
                    print(f"[AI] Função '{fn_name}' gerada e registada para '{source_code}'")
                else:
                    # AI falhou — cair no mapeamento manual
                    extract_function = manual_mapping.get(source_code)
                    if extract_function:
                        fn_source = "manual"
                        print(f"[AI] Geração falhou, a usar mapeamento manual '{extract_function}' para '{source_code}'")
                    else:
                        err_msg = (
                            f"AI não conseguiu gerar função para '{source_code}': {gen['error']}. "
                            f"Configure um mapeamento manual em Painel de Controlo."
                        )
                        cur.execute(
                            "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s WHERE file_id = %s",
                            (err_msg, file_id)
                        )
                        _log_error(cur, conn, file_id, "transform", err_msg)
                        conn.commit()
                        print(f"[ERRO] file_id={file_id}: {err_msg}")
                        err_count += 1
                        continue

        if extract_function not in EXTRACT_FUNCTIONS:
            err_msg = f"Função '{extract_function}' (mapeada para '{source_code}') não encontrada em EXTRACT_FUNCTIONS"
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
                "UPDATE op_data SET pipeline_status = 'SILVER_OK', "
                "transform_fn_name = %s, transform_fn_source = %s WHERE file_id = %s",
                (extract_function, fn_source, file_id)
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
