import csv
import json
import io
import os
import pandas as pd
import boto3
import psycopg2
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

from silver_functions import EXTRACT_FUNCTIONS, clean_dataframe
from silver_function_generator import generate_and_validate, save_to_auto_store
from config import DB_CONFIG, MINIO_CONFIG, BUCKET_RAW, BUCKET_SILVER

MAX_WORKERS = 8

_FALLBACK_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "silver_errors_fallback.log")


def _log_error(cur, conn, file_id, step: str, message: str):
    try:
        cur.execute(
            "INSERT INTO etl_logs_dados (file_id, step, error_message) VALUES (%s, %s, %s)",
            (file_id, step, message),
        )
    except Exception as log_exc:
        print(f"[ERRO-LOG] file_id={file_id} step={step}: {message}")
        print(f"[ERRO-LOG] Falha ao registar em etl_logs: {log_exc}")
        try:
            from datetime import datetime as _dt
            with open(_FALLBACK_LOG, "a", encoding="utf-8") as _f:
                _f.write(f"{_dt.now().isoformat()} | file_id={file_id} step={step}: {message}\n")
                _f.write(f"  DB error: {log_exc}\n")
        except Exception:
            pass


def _detect_format_from_content(content: bytes) -> str:
    """Fallback de deteção por conteúdo — usado quando o metadata Bronze não tem formato."""
    if content[:4] == b"PK\x03\x04":
        return "zip"
    if content[:4] == b"\xD0\xCF\x11\xE0":
        return "excel"
    try:
        snippet = content[:512].decode("utf-8", errors="strict").lstrip()
        if snippet.startswith(("{", "[")):
            return "json"
        if snippet.startswith("<"):
            return "xml"
        if "\n" in snippet:
            try:
                csv.Sniffer().sniff(snippet, delimiters=",;\t|")
                return "csv"
            except csv.Error:
                pass
    except UnicodeDecodeError:
        pass
    return "unknown"


def read_raw_object(s3, key: str):
    response = s3.get_object(Bucket=BUCKET_RAW, Key=key)
    content  = response["Body"].read()

    # Usa o formato guardado no metadata pelo bronze; só re-deteta se em falta
    fmt = response.get("Metadata", {}).get("format", "unknown")
    if not fmt or fmt == "unknown":
        fmt = _detect_format_from_content(content)

    if fmt == "json":
        return json.loads(content)
    elif fmt == "csv":
        return pd.read_csv(io.BytesIO(content))
    elif fmt in ("excel", "zip"):
        return pd.read_excel(io.BytesIO(content))
    elif fmt == "xml":
        try:
            return pd.read_xml(io.BytesIO(content))
        except Exception as xml_err:
            raise ValueError(
                f"Formato 'xml' não pôde ser lido automaticamente: {xml_err}. "
                "Configure uma função de transformação manual no Painel de Controlo."
            )
    else:
        raise ValueError(f"Formato '{fmt}' não suportado nas funções de transformação silver.")


_AI_SKIP = "manual"


def _transform_one(file_id: int, report_id, fn_name: str) -> tuple:
    """Executa a transformação de um ficheiro num thread dedicado."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    s3 = boto3.client("s3", **MINIO_CONFIG)
    key = str(file_id)

    try:
        data = read_raw_object(s3, key)
        df   = EXTRACT_FUNCTIONS[fn_name](data)

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
            "UPDATE op_data SET pipeline_status = 'silver' WHERE file_id = %s",
            (file_id,)
        )
        conn.commit()
        print(f"[OK]   {key} -> {key}.parquet")
        return True, None

    except Exception as e:
        err_msg = str(e)
        try:
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'failed' WHERE file_id = %s",
                (file_id,)
            )
            _log_error(cur, conn, file_id, "silver", err_msg)
            conn.commit()
        except Exception:
            pass
        print(f"[ERRO] {key}: {e}")
        return False, err_msg

    finally:
        cur.close()
        conn.close()


def transformar():
    s3 = boto3.client("s3", **MINIO_CONFIG)

    try:
        s3.head_bucket(Bucket=BUCKET_SILVER)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=BUCKET_SILVER)
        else:
            raise

    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    cur.execute("""
        SELECT d.file_id, d.report_id, r.source_code
        FROM op_data d
        JOIN op_report r ON r.report_id = d.report_id
        WHERE d.pipeline_status = 'bronze'
        ORDER BY d.file_id
        FOR UPDATE OF d SKIP LOCKED
    """)
    rows = cur.fetchall()

    if not rows:
        print("Sem ficheiros BRONZE para transformar.")
        cur.close(); conn.close()
        return

    file_ids = [r[0] for r in rows]
    cur.execute(
        "UPDATE op_data SET pipeline_status = 'processing' WHERE file_id = ANY(%s)",
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

    err_count = 0

    # --- Fase 1: Resolução de funções (sequencial) ---
    # Inclui geração AI e validação de mapeamentos.
    # Todos os writes a EXTRACT_FUNCTIONS e ai_mapping acontecem aqui,
    # antes do pool de threads — sem necessidade de locks.
    ready_to_transform = []  # (file_id, report_id, fn_name)

    for file_id, report_id, source_code in rows:
        key = str(file_id)
        ai_fn = ai_mapping.get(source_code)

        if ai_fn == _AI_SKIP:
            # Keyword "manual": ignora IA, usa função manual
            extract_function = manual_mapping.get(source_code)
            if not extract_function:
                err_msg = f"Sem mapeamento manual para '{source_code}'. Configure um mapeamento no Painel de Controlo."
                cur.execute("UPDATE op_data SET pipeline_status = 'failed' WHERE file_id = %s", (file_id,))
                _log_error(cur, conn, file_id, "silver", err_msg)
                conn.commit()
                print(f"[ERRO] file_id={file_id}: {err_msg}")
                err_count += 1
                continue

        elif ai_fn:
            # Função AI em cache
            extract_function = ai_fn

        else:
            # Sem cache AI: tentar gerar
            print(f"[AI] Sem mapeamento AI para '{source_code}', a gerar função para file_id={file_id}...")
            try:
                content = s3.get_object(Bucket=BUCKET_RAW, Key=key)["Body"].read()
                hint = hint_mapping.get(source_code)
                gen = generate_and_validate(content, hint=hint)
            except Exception as e:
                gen = {"valid": False, "error": str(e), "function_name": None, "code": None}

            if gen["valid"]:
                fn_name = gen["function_name"]
                try:
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
                    print(f"[AI] Função '{fn_name}' gerada e registada para '{source_code}'")
                except Exception as exec_err:
                    print(f"[AI] Erro ao carregar função gerada '{fn_name}': {exec_err}. A tentar mapeamento manual.")
                    extract_function = manual_mapping.get(source_code)
                    if not extract_function:
                        err_msg = f"AI gerou função mas falhou ao carregá-la para '{source_code}': {exec_err}. Configure um mapeamento manual no Painel de Controlo."
                        cur.execute("UPDATE op_data SET pipeline_status = 'failed' WHERE file_id = %s", (file_id,))
                        _log_error(cur, conn, file_id, "silver", err_msg)
                        conn.commit()
                        err_count += 1
                        continue
            else:
                extract_function = manual_mapping.get(source_code)
                if extract_function:
                    print(f"[AI] Geração falhou, a usar mapeamento manual '{extract_function}' para '{source_code}'")
                else:
                    err_msg = f"AI não conseguiu gerar função para '{source_code}': {gen['error']}. Configure um mapeamento manual no Painel de Controlo."
                    cur.execute("UPDATE op_data SET pipeline_status = 'failed' WHERE file_id = %s", (file_id,))
                    _log_error(cur, conn, file_id, "silver", err_msg)
                    conn.commit()
                    print(f"[ERRO] file_id={file_id}: {err_msg}")
                    err_count += 1
                    continue

        if extract_function not in EXTRACT_FUNCTIONS:
            err_msg = f"Função '{extract_function}' (mapeada para '{source_code}') não encontrada em EXTRACT_FUNCTIONS"
            cur.execute("UPDATE op_data SET pipeline_status = 'failed' WHERE file_id = %s", (file_id,))
            _log_error(cur, conn, file_id, "silver", err_msg)
            conn.commit()
            print(f"[ERRO] file_id={file_id}: {err_msg}")
            err_count += 1
            continue

        print(f"A transformar: {key} ({extract_function})")
        ready_to_transform.append((file_id, report_id, extract_function))

    cur.close()
    conn.close()

    # --- Fase 2: Transformação paralela ---
    # EXTRACT_FUNCTIONS está completo neste ponto — leituras seguras nos threads.
    ok_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_transform_one, file_id, report_id, fn_name): file_id
            for file_id, report_id, fn_name in ready_to_transform
        }
        for future in as_completed(futures):
            try:
                success, _ = future.result()
            except Exception as e:
                file_id = futures[future]
                print(f"[ERRO] file_id={file_id} exceção não tratada: {e}")
                success = False
            if success:
                ok_count += 1
            else:
                err_count += 1

    print(f"silver concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    transformar()
