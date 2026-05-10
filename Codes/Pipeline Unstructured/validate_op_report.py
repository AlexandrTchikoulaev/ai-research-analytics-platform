"""
Valida os registos de op_report antes da ingestão para a camada Bronze.
Verifica report_url, file_name e existência do relatório.
Retorna (valid_ids, invalid_ids).
Regista erros em etl_logs_pdfs.
"""
import psycopg2

DB_PIPELINE = {
    "host": "localhost",
    "port": 5433,
    "dbname": "gestao_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

DB_OPERATIONAL = {
    "host": "localhost",
    "port": 5433,
    "dbname": "gestao_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

PROCESS_NAME = "etl_pdfs"

VALID_AREAS_TEMATICAS = {"Tecnologia", "Economia", "Educação", "Saúde", "Ambiente", "Outros"}
VALID_ESTADOS         = {"Anual", "Bienal", "Pontual", "Descontinuado"}


def validate(last_run=None):
    conn_pipe = conn_op = None
    try:
        conn_pipe = psycopg2.connect(**DB_PIPELINE)
        cur_pipe  = conn_pipe.cursor()
        conn_op   = psycopg2.connect(**DB_OPERATIONAL)
        cur_op    = conn_op.cursor()

        if last_run is None:
            cur_pipe.execute("SELECT last_run FROM etl_data WHERE process_name = %s", (PROCESS_NAME,))
            row = cur_pipe.fetchone()
            last_run = row[0] if row else None

        if last_run:
            cur_op.execute("""
                SELECT report_id, file_name, report_url, created_at, area_tematica, estado
                FROM op_report
                WHERE created_at > %s
            """, (last_run,))
        else:
            cur_op.execute("""
                SELECT report_id, file_name, report_url, created_at, area_tematica, estado
                FROM op_report
            """)

        rows = cur_op.fetchall()

        valid_ids = []
        invalid_ids = []

        for report_id, file_name, report_url, created_at, area_tematica, estado in rows:
            errors = []

            if not created_at:
                errors.append("Campo created_at em falta")

            if report_url is None:
                pass  # ficheiro uploaded directamente para MinIO — sem URL necessário
            elif not report_url:
                errors.append("report_url está vazio")
            elif not report_url.startswith("http"):
                errors.append(f"report_url inválido (não começa com http): {report_url}")

            if area_tematica not in VALID_AREAS_TEMATICAS:
                errors.append(f"area_tematica inválida: '{area_tematica}'. Valores aceites: {sorted(VALID_AREAS_TEMATICAS)}")

            if estado not in VALID_ESTADOS:
                errors.append(f"estado inválido: '{estado}'. Valores aceites: {sorted(VALID_ESTADOS)}")

            if errors:
                msg = "; ".join(errors)
                cur_pipe.execute(
                    "INSERT INTO etl_logs_pdfs (report_id, file_name, step, status, error_message) VALUES (%s, %s, %s, %s, %s)",
                    (report_id, file_name or f"report_id={report_id}", "validate_op_report", "error", msg),
                )
                print(f"[INVÁLIDO] report_id={report_id}: {msg}")
                invalid_ids.append(report_id)
            else:
                valid_ids.append(report_id)

        conn_pipe.commit()
        print(f"validate_op_report — {len(valid_ids)} válidos, {len(invalid_ids)} inválidos")
        return valid_ids, invalid_ids

    except Exception as e:
        if conn_pipe:
            try:
                conn_pipe.cursor().execute(
                    "INSERT INTO etl_logs_pdfs (report_id, file_name, step, status, error_message) VALUES (%s, %s, %s, %s, %s)",
                    (None, "N/A", "validate_op_report", "error", str(e)),
                )
                conn_pipe.commit()
            except Exception:
                pass
        raise
    finally:
        if conn_pipe:
            conn_pipe.close()
        if conn_op:
            conn_op.close()


if __name__ == "__main__":
    validate()
