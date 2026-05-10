import psycopg2


def main():
    # -------------------------
    # Conexão
    # -------------------------
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="gestao_db",
        user="projeto_utilizador",
        password="projeto"
    )
    cur = conn.cursor()

    # -------------------------
    # Create Table
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS etl_logs_dados (
        id SERIAL PRIMARY KEY,
        file_id VARCHAR,
        file_name VARCHAR,
        step VARCHAR,
        status VARCHAR,
        error_message TEXT,
        log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS etl_logs_pdfs (
        id SERIAL PRIMARY KEY,
        report_id INTEGER,
        file_name VARCHAR,
        step VARCHAR,
        status VARCHAR,
        error_message TEXT,
        log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE OR REPLACE FUNCTION fill_etl_log_file_name()
    RETURNS TRIGGER AS $$
    BEGIN
        IF NEW.file_name IS NULL AND NEW.file_id IS NOT NULL THEN
            SELECT file_name INTO NEW.file_name
            FROM op_data
            WHERE file_id::text = NEW.file_id;
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """)

    cur.execute("""
    DROP TRIGGER IF EXISTS trg_fill_etl_log_file_name ON etl_logs_dados;
    """)

    cur.execute("""
    CREATE TRIGGER trg_fill_etl_log_file_name
    BEFORE INSERT ON etl_logs_dados
    FOR EACH ROW EXECUTE FUNCTION fill_etl_log_file_name();
    """)

    conn.commit()

    # -------------------------
    # Close Connection
    # -------------------------
    cur.close()
    conn.close()

    print("ETL logs tables (etl_logs_dados, etl_logs_pdfs) created successfully")


# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    main()