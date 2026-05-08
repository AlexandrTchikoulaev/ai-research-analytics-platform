import psycopg2


def main():
    # -------------------------
    # Conexão
    # -------------------------
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="operational_db",
        user="projeto_utilizador",
        password="projeto"
    )
    cur = conn.cursor()

    # -------------------------
    # Delete Tables
    # -------------------------
    tables = ["op_report", "op_data"]

    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        print(f"Tabela {table} apagada")

    conn.commit()
    print("Tables deleted!")

    # -------------------------
    # Create Tables
    # -------------------------

    # TABLE: op_report
    cur.execute("""
    CREATE TABLE op_report (
        report_id SERIAL PRIMARY KEY,
        file_name VARCHAR(100),
        source_code VARCHAR(50),
        report_url TEXT,
        publication_date DATE,
        area_tematica VARCHAR(100),
        estado VARCHAR(50),
        palavras_chave TEXT,
        resumo TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # TABLE: op_data
    cur.execute("""
    CREATE TABLE op_data (
        file_id SERIAL PRIMARY KEY,
        report_id INTEGER NOT NULL,
        file_name VARCHAR(100),
        file_url TEXT,
        extract_function TEXT,
        file_type VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        CONSTRAINT fk_report
            FOREIGN KEY(report_id)
            REFERENCES op_report(report_id)
            ON DELETE CASCADE
    );
    """)

    conn.commit()

    # -------------------------
    # Close Connection
    # -------------------------
    cur.close()
    conn.close()

    print("Operational tables created successfully")


# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    main()