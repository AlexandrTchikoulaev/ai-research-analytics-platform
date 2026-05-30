import psycopg2
import requests
import boto3
from botocore.exceptions import ClientError
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

_print_lock = threading.Lock()


def _log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)

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

BUCKET_UNSTRUCTURED = "bronze-unstructured"
BUCKET_THUMBNAILS   = "thumbnails"
MAX_WORKERS         = 5

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# Sessões HTTP isoladas por thread: cada worker tem o seu próprio dict de sessões,
# evitando partilha de estado não-thread-safe entre workers concorrentes.
_thread_local = threading.local()


def _get_session(url: str) -> requests.Session:
    if not hasattr(_thread_local, "sessions"):
        _thread_local.sessions = {}
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if origin not in _thread_local.sessions:
        session = requests.Session()
        session.headers.update(_BROWSER_HEADERS)
        try:
            session.get(origin, timeout=15, allow_redirects=True)
            time.sleep(1)
        except Exception:
            pass
        _thread_local.sessions[origin] = session
    return _thread_local.sessions[origin]


def _download_pdf(url: str, retries: int = 3) -> bytes:
    session = _get_session(url)
    parsed = urlparse(url)
    referer = f"{parsed.scheme}://{parsed.netloc}/"

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                url,
                timeout=60,
                allow_redirects=True,
                headers={"Accept": "application/pdf,*/*", "Referer": referer},
            )
            resp.raise_for_status()
            return resp.content
        except requests.HTTPError as e:
            retryable = e.response is not None and e.response.status_code in (403, 429)
            if retryable and attempt < retries:
                wait = 5 * attempt
                _log(f"  [{attempt}/{retries}] HTTP {e.response.status_code}, a aguardar {wait}s...")
                time.sleep(wait)
            else:
                raise
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < retries:
                wait = 5 * attempt
                _log(f"  [{attempt}/{retries}] Erro de rede ({type(e).__name__}), a aguardar {wait}s...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("Número máximo de tentativas atingido")


def _is_valid_pdf(content: bytes) -> bool:
    return b"%PDF" in content[:1024]


def _generate_thumbnail(s3, report_id: int, pdf_bytes: bytes) -> bool:
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if len(doc) == 0:
            doc.close()
            return False
        pix = doc[0].get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False)
        img_bytes = pix.tobytes("jpeg")
        doc.close()
        s3.put_object(
            Bucket=BUCKET_THUMBNAILS,
            Key=f"{report_id}.jpg",
            Body=img_bytes,
            ContentType="image/jpeg",
        )
        return True
    except ImportError:
        return False
    except Exception as e:
        _log(f"  [AVISO] Thumbnail report_id={report_id}: {e}")
        return False


def _db_execute(sql_calls: list[tuple]):
    """Abre uma ligação nova, executa os comandos e faz commit. Reconecta sempre."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        for sql, params in sql_calls:
            cur.execute(sql, params)
        conn.commit()
        cur.close()
    finally:
        conn.close()


def _process_one(
    report_id: int,
    file_name: str | None,
    report_url: str | None,
    existing_keys: frozenset,
) -> bool:
    """Descarrega e ingere um único relatório PDF. Devolve True se OK, False se erro.

    Cada worker cria o seu próprio cliente S3 e sessão HTTP — sem estado partilhado.
    """
    s3 = boto3.client("s3", **MINIO_CONFIG)

    if file_name is None:
        msg = "file_name é NULL — registo inválido"
        _db_execute([
            ("UPDATE op_report SET pipeline_status = 'FAILED', pipeline_error = %s WHERE report_id = %s",
             (msg, report_id)),
            ("INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
             (report_id, f"report_id={report_id}", "bronze", msg)),
        ])
        _log(f"[ERRO] report_id={report_id}: {msg}")
        return False

    if file_name in existing_keys:
        _db_execute([(
            "UPDATE op_report SET pipeline_status = 'BRONZE_OK' WHERE report_id = %s",
            (report_id,)
        )])
        _log(f"[OK]   report_id={report_id}  {file_name}  [já no bucket]")
        return True

    if report_url is None:
        msg = "report_url é NULL e ficheiro não encontrado no bucket — faça upload via /op_report/upload"
        _db_execute([
            ("UPDATE op_report SET pipeline_status = 'FAILED', pipeline_error = %s WHERE report_id = %s",
             (msg, report_id)),
            ("INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
             (report_id, file_name, "bronze", msg)),
        ])
        _log(f"[ERRO] report_id={report_id}: sem report_url e sem ficheiro no bucket")
        return False

    try:
        _log(f"  >> report_id={report_id}  A descarregar: {report_url}")
        content = _download_pdf(report_url)

        if not _is_valid_pdf(content):
            if content[:1] == b"<":
                raise ValueError(
                    "O servidor devolveu HTML em vez do PDF (provável bloqueio/CAPTCHA)."
                )
            raise ValueError(
                f"Conteúdo não é PDF válido (não contém %PDF nos primeiros 1024 bytes). "
                f"Primeiros bytes: {content[:16]!r}"
            )

        safe_name = file_name.encode("ascii", errors="replace").decode("ascii")
        s3.put_object(
            Bucket=BUCKET_UNSTRUCTURED,
            Key=file_name,
            Body=content,
            ContentType="application/pdf",
            Metadata={
                "report_id": str(report_id),
                "file_name": safe_name,
            },
        )
        _db_execute([(
            "UPDATE op_report SET pipeline_status = 'BRONZE_OK' WHERE report_id = %s",
            (report_id,)
        )])
        thumb_ok = _generate_thumbnail(s3, report_id, content)
        thumb_tag = " [thumb]" if thumb_ok else ""
        _log(f"[OK]   report_id={report_id}  {file_name}{thumb_tag}")
        return True

    except Exception as e:
        err_msg = str(e)
        try:
            _db_execute([
                ("UPDATE op_report SET pipeline_status = 'FAILED', pipeline_error = %s WHERE report_id = %s",
                 (err_msg, report_id)),
                ("INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                 (report_id, file_name, "bronze", err_msg)),
            ])
        except Exception as db_err:
            _log(f"  [AVISO] Não foi possível registar erro no DB: {db_err}")
        _log(f"[ERRO] report_id={report_id}  {file_name}: {e}")
        return False


def main():
    print("A correr bronze (ingest unstructured)...")

    s3 = boto3.client("s3", **MINIO_CONFIG)

    # Garantir que os buckets existem antes de lançar workers concorrentes,
    # evitando race conditions na criação.
    for bucket in (BUCKET_UNSTRUCTURED, BUCKET_THUMBNAILS):
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
                s3.create_bucket(Bucket=bucket)
            else:
                raise

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT report_id, file_name, report_url
            FROM op_report
            WHERE pipeline_status = 'PENDING'
            ORDER BY report_id
            FOR UPDATE SKIP LOCKED
        """)
        rows = cur.fetchall()

        if not rows:
            print("Sem relatórios PENDING para ingestão.")
            cur.close()
            return

        report_ids = [r[0] for r in rows]
        cur.execute(
            "UPDATE op_report SET pipeline_status = 'PROCESSING' WHERE report_id = ANY(%s)",
            (report_ids,)
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()

    existing_keys: frozenset = frozenset(
        obj["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET_UNSTRUCTURED)
        for obj in page.get("Contents", [])
    )

    ok_count  = 0
    err_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_process_one, report_id, file_name, report_url, existing_keys): report_id
            for report_id, file_name, report_url in rows
        }
        for future in as_completed(futures):
            report_id = futures[future]
            try:
                if future.result():
                    ok_count += 1
                else:
                    err_count += 1
            except Exception as e:
                _log(f"[ERRO INESPERADO] report_id={report_id}: {e}")
                err_count += 1

    print(f"bronze concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    main()
