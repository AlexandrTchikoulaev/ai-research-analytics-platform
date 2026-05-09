import psycopg2


def main():
    # -------------------------
    # Conexão
    # -------------------------
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="pipeline_db",
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
        file_id VARCHAR,
        file_name VARCHAR,
        step VARCHAR,
        status VARCHAR,
        error_message TEXT,
        log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
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