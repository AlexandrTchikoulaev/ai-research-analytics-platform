"""
Valida os registos de op_data antes da ingestão para a camada Bronze.
Retorna (valid_ids, invalid_ids).
Regista erros em etl_logs_dados.
"""
import psycopg2

DB_CONFIG = {
    "host": "localhost", "port": 5433, "dbname": "gestao_db",
    "user": "projeto_utilizador", "password": "projeto",
}

def validate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT file_id, report_id, file_url
        FROM op_data
        WHERE pipeline_status = 'PENDING'
    """)
    rows = cur.fetchall()

    valid_ids = []
    invalid_ids = []

    for file_id, report_id, file_url in rows:
        errors = []

        cur.execute("SELECT 1 FROM op_report WHERE report_id = %s", (report_id,))
        if not cur.fetchone():
            errors.append(f"report_id {report_id} não existe em op_report")

        if file_url and not file_url.startswith("http"):
            errors.append(f"file_url inválido: {file_url}")

        if errors:
            msg = "; ".join(errors)
            cur.execute("""
                UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s
                WHERE file_id = %s
            """, (msg, file_id))
            cur.execute("""
                INSERT INTO etl_logs_dados (file_id, step, error_message)
                VALUES (%s, %s, %s)
            """, (file_id, "validate_opdata", msg))
            conn.commit()
            print(f"[INVÁLIDO] file_id={file_id}: {msg}")
            invalid_ids.append(file_id)
        else:
            valid_ids.append(file_id)

    conn.commit()
    cur.close()
    conn.close()

    print(f"validate_opdata — {len(valid_ids)} válidos, {len(invalid_ids)} inválidos")
    return valid_ids, invalid_ids


if __name__ == "__main__":
    validate()
