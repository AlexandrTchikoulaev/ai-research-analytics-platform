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
    # Delete Table
    # -------------------------
    cur.execute("DROP TABLE IF EXISTS etl_data CASCADE;")
    print("Tabela etl_data apagada")

    # -------------------------
    # Create Table
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS etl_data (
        process_name VARCHAR PRIMARY KEY,
        last_run TIMESTAMP
    );
    """)

    # -------------------------
    # Insert Initial Row
    # -------------------------
    cur.execute("""
    INSERT INTO etl_data (process_name, last_run)
    VALUES ('etl_dados', '2000-01-01')
    ON CONFLICT (process_name) DO NOTHING;
    """)

    cur.execute("""
    INSERT INTO etl_data (process_name, last_run)
    VALUES ('etl_pdfs', '2000-01-01')
    ON CONFLICT (process_name) DO NOTHING;
    """)

    conn.commit()

    # -------------------------
    # Close Connection
    # -------------------------
    cur.close()
    conn.close()

    print("ETL control table created successfully")


# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    main()