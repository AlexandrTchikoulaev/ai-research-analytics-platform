import sys
import os
sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "Extra")))
from config import DB_GESTAO

import psycopg2


def main():
    conn = psycopg2.connect(**DB_GESTAO)
    cur = conn.cursor()

    # -------------------------
    # Delete Tables
    # -------------------------
    for table in ["etl_logs_dados", "etl_logs_pdfs", "op_report", "op_data"]:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        print(f"Tabela {table} apagada")
    conn.commit()

    # -------------------------
    # op_report
    # -------------------------
    cur.execute("""
    CREATE TABLE op_report (
        report_id        SERIAL PRIMARY KEY,
        file_name        VARCHAR(100),
        source_code      VARCHAR(50),
        report_url       TEXT UNIQUE,
        publication_date DATE,
        area_tematica    VARCHAR(100),
        estado           VARCHAR(50),
        palavras_chave   TEXT,
        resumo           TEXT,
        pipeline_status  TEXT NOT NULL DEFAULT 'pending'
    );
    """)

    # -------------------------
    # op_data
    # -------------------------
    cur.execute("""
    CREATE TABLE op_data (
        file_id          SERIAL PRIMARY KEY,
        report_id        INTEGER NOT NULL,
        file_url         TEXT,
        pipeline_status  TEXT NOT NULL DEFAULT 'pending',
        CONSTRAINT fk_report
            FOREIGN KEY(report_id) REFERENCES op_report(report_id) ON DELETE CASCADE
    );
    """)

    cur.execute("""
    CREATE TABLE source_function_mapping (
        source_code          TEXT PRIMARY KEY,
        extract_function     TEXT,
        ai_extract_function  TEXT,
        generation_hint      TEXT
    );
    """)

    # -------------------------
    # etl_logs_dados
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS etl_logs_dados (
        id            SERIAL PRIMARY KEY,
        file_id       VARCHAR,
        file_name     VARCHAR,
        step          VARCHAR,
        error_message TEXT,
        log_time      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # -------------------------
    # etl_logs_pdfs
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS etl_logs_pdfs (
        id            SERIAL PRIMARY KEY,
        report_id     INTEGER,
        file_name     VARCHAR,
        step          VARCHAR,
        error_message TEXT,
        log_time      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # -------------------------
    # Trigger: preencher file_name em etl_logs_dados
    # -------------------------
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

    cur.execute("DROP TRIGGER IF EXISTS trg_fill_etl_log_file_name ON etl_logs_dados;")

    cur.execute("""
    CREATE TRIGGER trg_fill_etl_log_file_name
    BEFORE INSERT ON etl_logs_dados
    FOR EACH ROW EXECUTE FUNCTION fill_etl_log_file_name();
    """)

    cur.execute("ALTER TABLE etl_logs_dados DROP COLUMN IF EXISTS status;")
    cur.execute("ALTER TABLE etl_logs_pdfs  DROP COLUMN IF EXISTS status;")

    conn.commit()
    cur.close()
    conn.close()

    print("gestao_db: todas as tabelas criadas com sucesso")


if __name__ == "__main__":
    main()
