import psycopg2
import requests
import boto3

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


def main():
    print("A correr bronze (ingest unstructured)...")

    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    s3   = boto3.client("s3", **MINIO_CONFIG)

    try:
        s3.head_bucket(Bucket=BUCKET_UNSTRUCTURED)
    except Exception:
        s3.create_bucket(Bucket=BUCKET_UNSTRUCTURED)

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

    # Pre-fetch existing bucket keys
    existing_keys = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_UNSTRUCTURED):
        for obj in page.get("Contents", []):
            existing_keys.add(obj["Key"])

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/pdf,*/*",
    })
    ok_count  = 0
    err_count = 0

    for report_id, file_name, report_url in rows:

        # Ficheiro já carregado via upload (ou já existe no bucket)
        if file_name in existing_keys:
            cur.execute(
                "UPDATE op_report SET pipeline_status = 'BRONZE_OK' WHERE report_id = %s",
                (report_id,)
            )
            conn.commit()
            print(f"[OK]   report_id={report_id}  [já no bucket: {file_name}]")
            ok_count += 1
            continue

        if report_url is None:
            msg = "report_url é NULL e ficheiro não encontrado no bucket — faça upload via /op_report/upload"
            cur.execute(
                "UPDATE op_report SET pipeline_status = 'FAILED', pipeline_error = %s WHERE report_id = %s",
                (msg, report_id)
            )
            cur.execute(
                "INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                (report_id, file_name, "bronze", msg),
            )
            conn.commit()
            print(f"[ERRO] report_id={report_id}: sem report_url e sem ficheiro no bucket")
            err_count += 1
            continue

        try:
            print(f"A descarregar: {report_url}")
            response = session.get(report_url, timeout=60, allow_redirects=True)
            response.raise_for_status()
            content = response.content

            if not _is_valid_pdf(content):
                ct = response.headers.get("Content-Type", "desconhecido")
                if "html" in ct.lower() or content[:1] == b"<":
                    raise ValueError(
                        f"O servidor devolveu HTML em vez do PDF (bloqueio/CAPTCHA). Content-Type: {ct}"
                    )
                raise ValueError(f"Conteúdo não é PDF válido (não começa com %PDF). Content-Type: {ct}")

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
            cur.execute(
                "UPDATE op_report SET pipeline_status = 'BRONZE_OK' WHERE report_id = %s",
                (report_id,)
            )
            conn.commit()
            _generate_thumbnail(s3, report_id, content)
            print(f"[OK]   report_id={report_id}  {file_name}")
            ok_count += 1

        except Exception as e:
            err_msg = str(e)
            cur.execute(
                "UPDATE op_report SET pipeline_status = 'FAILED', pipeline_error = %s WHERE report_id = %s",
                (err_msg, report_id)
            )
            cur.execute(
                "INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                (report_id, file_name, "bronze", err_msg),
            )
            conn.commit()
            print(f"[ERRO] report_id={report_id}  {file_name}: {e}")
            err_count += 1

    cur.close(); conn.close()
    print(f"bronze concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    main()
