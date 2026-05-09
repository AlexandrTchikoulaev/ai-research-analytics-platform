import psycopg2


def main():
    # -------------------------
    # Conexão
    # -------------------------
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="warehouse_db",
        user="projeto_utilizador",
        password="projeto"
    )
    cur = conn.cursor()

    # -------------------------
    # Delete Tables
    # -------------------------
    tables = [
        "dim_indicator",
        "dim_location",
        "dim_location_hierarchy",
        "dim_date",
        "dim_report",
        "fact_values"
    ]

    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        print(f"Tabela {table} apagada")

    conn.commit()
    cur.close()
    conn.close()

    print("Tables deleted!")

    # -------------------------
    # Nova conexão
    # -------------------------
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="warehouse_db",
        user="projeto_utilizador",
        password="projeto"
    )
    cur = conn.cursor()

    # -------------------------
    # Create Tables
    # -------------------------

    # Dimension Indicators
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_indicator (
        indicator_code VARCHAR(100) PRIMARY KEY,
        indicator_name VARCHAR(255)
    );
    """)

    # Dimension Locations
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_location (
        location_code VARCHAR(100) PRIMARY KEY,
        location_name VARCHAR(255)
    );
    """)

    # Bridge Location Hierarchy
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_location_hierarchy (
        parent_location_code VARCHAR(100) REFERENCES dim_location(location_code),
        child_location_code VARCHAR(100) REFERENCES dim_location(location_code),
        relationship_type VARCHAR(100),
        PRIMARY KEY(parent_location_code, child_location_code)
    );
    """)

    # Dimension Date
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id SERIAL PRIMARY KEY,
        year INT UNIQUE
    );
    """)

    # Fact Indicator Values
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_values (
        report_id INTEGER,
        location_code VARCHAR(100) NOT NULL REFERENCES dim_location(location_code),
        indicator_code VARCHAR(100) NOT NULL REFERENCES dim_indicator(indicator_code),
        date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
        value NUMERIC,
        value_type VARCHAR(100),
        PRIMARY KEY (report_id, location_code, indicator_code, date_id)
    );
    """)

    # Dimension Reports
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_report (
        report_id SERIAL PRIMARY KEY,
        source_code VARCHAR(100),
        source_name VARCHAR(255),
        report_url VARCHAR(255),
        publication_date TIMESTAMP
    );
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("Tables created successfully")


# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    main()