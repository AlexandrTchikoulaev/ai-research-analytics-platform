"""
Valida os registos de op_data antes da ingestão para a camada Bronze.
Retorna (valid_ids, invalid_ids).
Regista erros em etl_logs_dados.
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

PROCESS_NAME = "etl_dados"

VALID_FILE_TYPES = {"indicator", "countries", "regions", "groups", "value"}


def _get_extract_functions():
    from silver_functions import EXTRACT_FUNCTIONS
    return set(EXTRACT_FUNCTIONS.keys())


def validate(last_run=None):
    conn_pipe = psycopg2.connect(**DB_PIPELINE)
    cur_pipe  = conn_pipe.cursor()

    conn_op = psycopg2.connect(**DB_OPERATIONAL)
    cur_op  = conn_op.cursor()

    if last_run is None:
        cur_pipe.execute("SELECT last_run FROM etl_data WHERE process_name = %s", (PROCESS_NAME,))
        row = cur_pipe.fetchone()
        last_run = row[0] if row else None

    if last_run:
        cur_op.execute("""
            SELECT file_id, report_id, file_url, extract_function, file_type, created_at
            FROM op_data
            WHERE created_at > %s
        """, (last_run,))
    else:
        cur_op.execute("""
            SELECT file_id, report_id, file_url, extract_function, file_type, created_at
            FROM op_data
        """)

    rows = cur_op.fetchall()

    try:
        known_functions = _get_extract_functions()
    except Exception:
        known_functions = set()

    valid_ids = []
    invalid_ids = []

    for file_id, report_id, file_url, extract_function, file_type, created_at in rows:
        errors = []

        if not created_at:
            errors.append("Campo created_at em falta")

        cur_op.execute("SELECT 1 FROM op_report WHERE report_id = %s", (report_id,))
        if not cur_op.fetchone():
            errors.append(f"report_id {report_id} não existe em op_report")

        if file_url and not file_url.startswith("http"):
            errors.append(f"file_url inválido: {file_url}")

        if extract_function and known_functions and extract_function not in known_functions:
            errors.append(f"extract_function desconhecida: {extract_function}")

        if file_type and file_type not in VALID_FILE_TYPES:
            errors.append(f"file_type inválido: {file_type} (válidos: {VALID_FILE_TYPES})")

        if errors:
            msg = "; ".join(errors)
            cur_pipe.execute("""
                INSERT INTO etl_logs_dados (file_id, step, status, error_message)
                VALUES (%s, %s, %s, %s)
            """, (file_id, "validate_opdata", "error", msg))
            print(f"[INVÁLIDO] file_id={file_id}: {msg}")
            invalid_ids.append(file_id)
        else:
            valid_ids.append(file_id)

    conn_pipe.commit()
    cur_pipe.close(); conn_pipe.close()
    cur_op.close();   conn_op.close()

    print(f"validate_opdata — {len(valid_ids)} válidos, {len(invalid_ids)} inválidos")
    return valid_ids, invalid_ids


if __name__ == "__main__":
    validate()
