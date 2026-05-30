"""
Valida os registos de op_data antes da ingestão para a camada Bronze.
Retorna (valid_ids, invalid_ids).
Regista erros em etl_logs_dados.
"""
import psycopg2
from urllib.parse import urlparse
from config import DB_CONFIG


def validate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT file_id, report_id, file_url
        FROM op_data
        WHERE pipeline_status = 'PENDING'
    """)
    rows = cur.fetchall()

    if not rows:
        cur.close(); conn.close()
        print("validate_opdata — 0 válidos, 0 inválidos")
        return [], []

    # Batch FK check: uma query em vez de N
    report_ids = [r[1] for r in rows if r[1] is not None]
    if report_ids:
        cur.execute("SELECT report_id FROM op_report WHERE report_id = ANY(%s)", (report_ids,))
        existing_report_ids = {r[0] for r in cur.fetchall()}
    else:
        existing_report_ids = set()

    valid_ids = []
    invalid_items = []  # (file_id, msg)

    for file_id, report_id, file_url in rows:
        errors = []

        if report_id is None:
            errors.append("report_id é NULL")
        elif report_id not in existing_report_ids:
            errors.append(f"report_id {report_id} não existe em op_report")

        if file_url:
            parsed = urlparse(file_url)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                errors.append(f"file_url inválido: {file_url}")

        if errors:
            msg = "; ".join(errors)
            print(f"[INVÁLIDO] file_id={file_id}: {msg}")
            invalid_items.append((file_id, msg))
        else:
            valid_ids.append(file_id)

    # Batch updates + commit único em vez de N commits
    if valid_ids:
        cur.execute(
            "UPDATE op_data SET pipeline_status = 'VALIDATED' WHERE file_id = ANY(%s)",
            (valid_ids,)
        )

    for file_id, msg in invalid_items:
        cur.execute(
            "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s WHERE file_id = %s",
            (msg, file_id)
        )
        cur.execute(
            "INSERT INTO etl_logs_dados (file_id, step, error_message) VALUES (%s, %s, %s)",
            (file_id, "validate_opdata", msg)
        )

    conn.commit()
    cur.close()
    conn.close()

    print(f"validate_opdata — {len(valid_ids)} válidos, {len(invalid_items)} inválidos")
    return valid_ids, [fid for fid, _ in invalid_items]


if __name__ == "__main__":
    validate()
