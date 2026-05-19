"""
Valida os registos de op_report antes da ingestão para a camada Bronze.
Verifica report_url, area_tematica e estado.
Marca pipeline_status = 'FAILED' para inválidos e regista erros em etl_logs_pdfs.
"""
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "gestao_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

VALID_AREAS_TEMATICAS = {"Tecnologia", "Economia", "Educação", "Saúde", "Ambiente", "Outros"}
VALID_ESTADOS         = {"Anual", "Bienal", "Pontual", "Descontinuado"}


def validate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT report_id, file_name, report_url, area_tematica, estado
        FROM op_report
        WHERE pipeline_status = 'PENDING'
    """)
    rows = cur.fetchall()

    valid_count   = 0
    invalid_count = 0

    for report_id, file_name, report_url, area_tematica, estado in rows:
        errors = []

        if report_url is None:
            pass  # ficheiro uploaded directamente para MinIO — sem URL necessário
        elif not report_url:
            errors.append("report_url está vazio")
        elif not report_url.startswith("http"):
            errors.append(f"report_url inválido (não começa com http): {report_url}")

        if area_tematica and area_tematica not in VALID_AREAS_TEMATICAS:
            errors.append(f"area_tematica inválida: '{area_tematica}'. Valores aceites: {sorted(VALID_AREAS_TEMATICAS)}")

        if estado and estado not in VALID_ESTADOS:
            errors.append(f"estado inválido: '{estado}'. Valores aceites: {sorted(VALID_ESTADOS)}")

        if errors:
            msg = "; ".join(errors)
            cur.execute(
                "UPDATE op_report SET pipeline_status = 'FAILED', pipeline_error = %s WHERE report_id = %s",
                (msg, report_id)
            )
            cur.execute(
                "INSERT INTO etl_logs_pdfs (report_id, file_name, step, error_message) VALUES (%s, %s, %s, %s)",
                (report_id, file_name or f"report_id={report_id}", "validate_op_report", msg),
            )
            conn.commit()
            print(f"[INVÁLIDO] report_id={report_id}: {msg}")
            invalid_count += 1
        else:
            valid_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"validate_op_report — {valid_count} válidos, {invalid_count} inválidos")


if __name__ == "__main__":
    validate()
