import sys
import os
sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts")))
from config import DB_WAREHOUSE

import logging
import psycopg2
import psycopg2.extras
import pycountry
import country_converter as coco

logging.getLogger('country_converter').setLevel(logging.ERROR)


def main():
    conn = psycopg2.connect(**DB_WAREHOUSE)
    cur = conn.cursor()

    # -------------------------
    # Delete Tables
    # -------------------------
    tables = [
        "fact_values",
        "dim_indicator",
        "dim_location",
        "dim_date",
        "dim_report",
    ]
    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        print(f"Tabela {table} apagada")
    conn.commit()

    # -------------------------
    # Create Tables
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_indicator (
        indicator_sk   SERIAL PRIMARY KEY,
        source_system  VARCHAR(100) NOT NULL,
        indicator_code VARCHAR(100) NOT NULL,
        indicator_name VARCHAR(255),
        UNIQUE (source_system, indicator_code)
    );
    """)

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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id SERIAL PRIMARY KEY,
        year INT UNIQUE
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_report (
        report_id        SERIAL PRIMARY KEY,
        source_code      VARCHAR(100),
        source_name      VARCHAR(255),
        report_url       VARCHAR(255),
        publication_date TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_values (
        report_id    INTEGER,
        location_sk  INTEGER NOT NULL REFERENCES dim_location(location_sk),
        indicator_sk INTEGER NOT NULL REFERENCES dim_indicator(indicator_sk),
        date_id      INTEGER NOT NULL REFERENCES dim_date(date_id),
        value        NUMERIC,
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
            sub_region if sub_region != 'not found' else None,
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

    # -------------------------
    # Create Views
    # -------------------------
    cur.execute("DROP VIEW IF EXISTS vw_indicator_location_year;")

    cur.execute("""
        CREATE VIEW vw_indicator_location_year AS
        SELECT
            di.indicator_name,
            dl.name          AS location_name,
            fv.value,
            dd.year
        FROM fact_values fv
        JOIN dim_indicator di ON fv.indicator_sk = di.indicator_sk
        JOIN dim_location   dl ON fv.location_sk  = dl.location_sk
        JOIN dim_date        dd ON fv.date_id      = dd.date_id;
    """)
    conn.commit()
    print("View vw_indicator_location_year criada")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
