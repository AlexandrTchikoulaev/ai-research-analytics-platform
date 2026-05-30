import csv
import time
import threading
import requests
import psycopg2
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from config import DB_CONFIG, MINIO_CONFIG, BUCKET_RAW

MAX_WORKERS = 8
MAX_PER_DOMAIN = 8   # pedidos concorrentes máximos por domínio externo
MAX_RETRIES = 3
_RETRY_DELAYS = [5, 15, 30]  # segundos entre tentativas para 5xx/429

# Semáforos por domínio para evitar rate-limiting
_domain_sems: dict = {}
_domain_sems_lock = threading.Lock()

_HTTP_HEADERS = {
    "Accept": "application/json, text/csv, application/vnd.ms-excel, */*",
    "Referer": "https://www.imf.org/",
}


def _get_domain_sem(url: str) -> threading.Semaphore:
    try:
        host = urlparse(url).netloc
    except Exception:
        host = url
    with _domain_sems_lock:
        if host not in _domain_sems:
            _domain_sems[host] = threading.Semaphore(MAX_PER_DOMAIN)
    return _domain_sems[host]


def detect_format(url: str, content: bytes) -> str:
    url_lower = url.lower().split("?")[0]
    if url_lower.endswith(".csv"):
        return "csv"
    if url_lower.endswith(".json"):
        return "json"
    if url_lower.endswith(".xlsx") or url_lower.endswith(".xls"):
        return "excel"
    if url_lower.endswith(".xml"):
        return "xml"
    if url_lower.endswith(".zip"):
        return "zip"

    # Magic bytes
    if content[:4] == b"PK\x03\x04":
        return "zip"
    if content[:4] == b"\xD0\xCF\x11\xE0":
        return "excel"

    # Text-based detection
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


def _download_with_retry(file_id: int, file_url: str) -> bytes:
    """Descarrega um URL com retry + backoff + semáforo por domínio."""
    sem = _get_domain_sem(file_url)
    with sem:
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(file_url, timeout=30, headers=_HTTP_HEADERS)
                response.raise_for_status()
                return response.content
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                retryable = status in (429, 500, 502, 503, 504)
                if retryable and attempt < MAX_RETRIES - 1:
                    delay = _RETRY_DELAYS[attempt]
                    print(f"[AVISO] file_id={file_id} HTTP {status}, retry {attempt + 1}/{MAX_RETRIES} em {delay}s")
                    time.sleep(delay)
                else:
                    raise
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt < MAX_RETRIES - 1:
                    delay = _RETRY_DELAYS[attempt]
                    print(f"[AVISO] file_id={file_id} erro de rede ({type(e).__name__}), retry {attempt + 1}/{MAX_RETRIES} em {delay}s")
                    time.sleep(delay)
                else:
                    raise
    # nunca alcançado mas satisfaz o type-checker
    raise RuntimeError(f"file_id={file_id}: download falhou após {MAX_RETRIES} tentativas")


def _ingest_one(file_id: int, report_id, file_url: str) -> tuple:
    """Ingere um ficheiro num thread dedicado com ligação DB e S3 próprias."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    s3 = boto3.client("s3", **MINIO_CONFIG)

    try:
        # Ficheiro já carregado via upload — já está no MinIO
        if not file_url:
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'BRONZE_OK' WHERE file_id = %s",
                (file_id,)
            )
            conn.commit()
            print(f"[OK]   file_id={file_id}  [upload direto]")
            return True, None

        content = _download_with_retry(file_id, file_url)

        fmt = detect_format(file_url, content)
        key = str(file_id)

        s3.put_object(
            Bucket=BUCKET_RAW,
            Key=key,
            Body=content,
            Metadata={
                "report_id": str(report_id) if report_id is not None else "",
                "format": fmt,
            },
        )
        cur.execute(
            "UPDATE op_data SET pipeline_status = 'BRONZE_OK' WHERE file_id = %s",
            (file_id,)
        )
        conn.commit()
        print(f"[OK]   file_id={file_id}  ({fmt})")
        return True, None

    except Exception as e:
        err_msg = str(e)
        try:
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s WHERE file_id = %s",
                (err_msg, file_id)
            )
            cur.execute(
                "INSERT INTO etl_logs_dados (file_id, step, error_message) VALUES (%s, %s, %s)",
                (file_id, "ingest_raw", err_msg)
            )
            conn.commit()
        except Exception:
            pass
        print(f"[ERRO] file_id={file_id}  {e}")
        return False, err_msg

    finally:
        cur.close()
        conn.close()


def main():
    print("A correr bronze...")

    s3 = boto3.client("s3", **MINIO_CONFIG)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        s3.head_bucket(Bucket=BUCKET_RAW)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=BUCKET_RAW)
        else:
            raise

    cur.execute("""
        SELECT file_id, report_id, file_url
        FROM op_data
        WHERE pipeline_status = 'VALIDATED'
        ORDER BY file_id
        FOR UPDATE SKIP LOCKED
    """)
    rows = cur.fetchall()

    if not rows:
        print("Sem ficheiros VALIDATED para ingerir.")
        cur.close(); conn.close()
        return

    file_ids = [r[0] for r in rows]
    cur.execute(
        "UPDATE op_data SET pipeline_status = 'PROCESSING' WHERE file_id = ANY(%s)",
        (file_ids,)
    )
    conn.commit()
    cur.close()
    conn.close()

    ok_count = 0
    err_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_ingest_one, file_id, report_id, file_url): file_id
            for file_id, report_id, file_url in rows
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

    print(f"ingest_raw concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    main()
