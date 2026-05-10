import logging
import psycopg2
import psycopg2.extras
import pycountry
import country_converter as coco

logging.getLogger('country_converter').setLevel(logging.ERROR)


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
        "fact_values",
        "dim_location_hierarchy",
        "dim_indicator",
        "dim_location",
        "dim_date",
        "dim_report"
    ]

    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        print(f"Tabela {table} apagada")

    conn.commit()

    # -------------------------
    # Create Tables
    # -------------------------

    # Dimension Indicators
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_indicator (
        indicator_sk   SERIAL PRIMARY KEY,
        source_system  VARCHAR(100) NOT NULL,
        indicator_code VARCHAR(100) NOT NULL,
        indicator_name VARCHAR(255),
        UNIQUE (source_system, indicator_code)
    );
    """)

    # Dimension Locations
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_location (
        location_sk   SERIAL PRIMARY KEY,
        location_code VARCHAR(100) UNIQUE NOT NULL,
        iso_alpha2    CHAR(2),
        iso_alpha3    CHAR(3),
        iso_numeric   CHAR(3),
        name          VARCHAR(255),
        region        VARCHAR(100),
        sub_region    VARCHAR(100)
    );
    """)

    # Bridge Location Hierarchy
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_location_hierarchy (
        parent_location_sk INTEGER REFERENCES dim_location(location_sk),
        child_location_sk  INTEGER REFERENCES dim_location(location_sk),
        relationship_type  VARCHAR(100),
        PRIMARY KEY (parent_location_sk, child_location_sk)
    );
    """)

    # Dimension Date
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id SERIAL PRIMARY KEY,
        year INT UNIQUE
    );
    """)

    # Dimension Reports
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_report (
        report_id        SERIAL PRIMARY KEY,
        source_code      VARCHAR(100),
        source_name      VARCHAR(255),
        report_url       VARCHAR(255),
        publication_date TIMESTAMP
    );
    """)

    # Fact Values
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_values (
        report_id    INTEGER,
        location_sk  INTEGER NOT NULL REFERENCES dim_location(location_sk),
        indicator_sk INTEGER NOT NULL REFERENCES dim_indicator(indicator_sk),
        date_id      INTEGER NOT NULL REFERENCES dim_date(date_id),
        value        NUMERIC,
        value_type   VARCHAR(100),
        PRIMARY KEY (report_id, location_sk, indicator_sk, date_id)
    );
    """)

    conn.commit()
    print("Tables created successfully")

    # -------------------------
    # Seed dim_location (ISO 3166 + UN M.49)
    # -------------------------
    cc = coco.CountryConverter()

    seed_data = []
    for c in pycountry.countries:
        region     = cc.convert(c.alpha_3, to='continent')
        sub_region = cc.convert(c.alpha_3, to='UNregion')
        seed_data.append((
            c.alpha_3,
            getattr(c, 'alpha_2', None),
            c.alpha_3,
            c.numeric,
            c.name,
            region     if region     != 'not found' else None,
            sub_region if sub_region != 'not found' else None
        ))

    psycopg2.extras.execute_values(cur, """
        INSERT INTO dim_location (location_code, iso_alpha2, iso_alpha3, iso_numeric, name, region, sub_region)
        VALUES %s
        ON CONFLICT (location_code) DO NOTHING
    """, seed_data, page_size=500)

    conn.commit()
    print(f"dim_location populada com {len(seed_data)} países ISO 3166")

    # -------------------------
    # Seed dim_date (1750–2040)
    # -------------------------
    years = [(year,) for year in range(1750, 2041)]
    psycopg2.extras.execute_values(cur, """
        INSERT INTO dim_date (year)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, years, page_size=500)

    conn.commit()
    print(f"dim_date populada com {len(years)} anos (1750–2040)")

    cur.close()
    conn.close()


# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    main()
