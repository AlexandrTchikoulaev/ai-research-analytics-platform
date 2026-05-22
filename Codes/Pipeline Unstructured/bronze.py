import psycopg2
import requests
import boto3
import time
from urllib.parse import urlparse

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

# Uma sessão por domínio para reutilizar cookies
_domain_sessions: dict[str, requests.Session] = {}


def _get_session(url: str) -> requests.Session:
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if origin not in _domain_sessions:
        session = requests.Session()
        session.headers.update(_BROWSER_HEADERS)
        # Warm-up: visita a homepage para obter cookies e parecer um browser real
        try:
            session.get(origin, timeout=15, allow_redirects=True)
            time.sleep(1)
        except Exception:
            pass
        _domain_sessions[origin] = session
    return _domain_sessions[origin]


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
            if e.response is not None and e.response.status_code in (403, 429) and attempt < retries:
                wait = 5 * attempt
                print(f"  [{attempt}/{retries}] HTTP {e.response.status_code}, a aguardar {wait}s...")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Número máximo de tentativas atingido")


def _is_valid_pdf(content: bytes) -> bool:
    return content[:4] == b"%PDF"


def _generate_thumbnail(s3, report_id: int, pdf_bytes: bytes):
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if len(doc) == 0:
            doc.close()
            return
        pix = doc[0].get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False)
        img_bytes = pix.tobytes("jpeg")
        doc.close()
        try:
            s3.head_bucket(Bucket=BUCKET_THUMBNAILS)
        except Exception:
            s3.create_bucket(Bucket=BUCKET_THUMBNAILS)
        s3.put_object(
            Bucket=BUCKET_THUMBNAILS,
            Key=f"{report_id}.jpg",
            Body=img_bytes,
            ContentType="image/jpeg",
        )
        print(f"[THUMB] Thumbnail gerado: report_id={report_id}")
    except ImportError:
        pass
    except Exception as e:
        print(f"[THUMB] Erro report_id={report_id}: {e}")


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


def main():
    print("A correr bronze (ingest unstructured)...")

    s3 = boto3.client("s3", **MINIO_CONFIG)

    try:
        s3.head_bucket(Bucket=BUCKET_UNSTRUCTURED)
    except Exception:
        s3.create_bucket(Bucket=BUCKET_UNSTRUCTURED)

    # Usa uma conexão dedicada só para leitura inicial e marcação PROCESSING
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
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
        cur.close(); conn.close()
        return

    report_ids = [r[0] for r in rows]
    cur.execute(
        "UPDATE op_report SET pipeline_status = 'PROCESSING' WHERE report_id = ANY(%s)",
        (report_ids,)
    )
    conn.commit()
    cur.close(); conn.close()

    # Pre-fetch existing bucket keys
    existing_keys = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_UNSTRUCTURED):
        for obj in page.get("Contents", []):
            existing_keys.add(obj["Key"])

    ok_count  = 0
    err_count = 0

    for report_id, file_name, report_url in rows:

        # Ficheiro já carregado via upload (ou já existe no bucket)
        if file_name in existing_keys:
            _db_execute([(
                "UPDATE op_report SET pipeline_status = 'BRONZE_OK' WHERE report_id = %s",
                (report_id,)
            )])
            print(f"[OK]   report_id={report_id}  [já no bucket: {file_name}]")
            ok_count += 1
            continue

        if report_url is None:
            msg = "report_url é NULL e ficheiro não encontrado no bucket — faça upload via /op_report/upload"
            _db_execute([
                ("UPDATE op_report SET pipeline_status = 'FAILED', pipeline_error = %s WHERE report_id = %s",
                 (msg, report_id)),
                ("INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                 (report_id, file_name, "bronze", msg)),
            ])
            print(f"[ERRO] report_id={report_id}: sem report_url e sem ficheiro no bucket")
            err_count += 1
            continue

        try:
            print(f"A descarregar: {report_url}")
            content = _download_pdf(report_url)

            if not _is_valid_pdf(content):
                ct_guess = "text/html" if content[:1] == b"<" else "desconhecido"
                if content[:1] == b"<":
                    raise ValueError(
                        f"O servidor devolveu HTML em vez do PDF (bloqueio/CAPTCHA). Content-Type: {ct_guess}"
                    )
                raise ValueError(f"Conteúdo não é PDF válido (não começa com %PDF).")

            s3.put_object(
                Bucket=BUCKET_UNSTRUCTURED,
                Key=file_name,
                Body=content,
                ContentType="application/pdf",
                Metadata={
                    "report_id": str(report_id),
                    "file_name": file_name,
                },
            )
            _db_execute([(
                "UPDATE op_report SET pipeline_status = 'BRONZE_OK' WHERE report_id = %s",
                (report_id,)
            )])
            _generate_thumbnail(s3, report_id, content)
            print(f"[OK]   report_id={report_id}  {file_name}")
            ok_count += 1

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
                print(f"  [AVISO] Não foi possível registar erro no DB: {db_err}")
            print(f"[ERRO] report_id={report_id}  {file_name}: {e}")
            err_count += 1

    print(f"bronze concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    main()
