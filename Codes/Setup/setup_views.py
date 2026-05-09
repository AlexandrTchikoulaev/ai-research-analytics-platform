import psycopg2


def main():
    # -------------------------
    # 🔌 Conexão
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
    # 🗑️ Apagar view (se existir)
    # -------------------------
    cur.execute("""
        DROP VIEW IF EXISTS vw_indicator_location_year;
    """)
    print("View antiga apagada (se existia)")

    # -------------------------
    # 🧱 Criar view
    # -------------------------
    cur.execute("""
        CREATE VIEW view AS
        SELECT
            di.indicator_name,
            dl.location_name,
            fv.value,
            dd.year
        FROM fact_values fv
        JOIN dim_indicator di 
            ON fv.indicator_code = di.indicator_code
        JOIN dim_location dl 
            ON fv.location_code = dl.location_code
        JOIN dim_date dd 
            ON fv.date_id = dd.date_id;
    """)

    print("View criada com sucesso 🚀")

    # -------------------------
    # 💾 Commit & close
    # -------------------------
    conn.commit()
    cur.close()
    conn.close()


# -------------------------
# ▶️ Entry point
# -------------------------
if __name__ == "__main__":
    main()