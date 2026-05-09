import psycopg2

def drop_tables(table_list):
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",          # ajusta se necessário
            database="warehouse_db",
            user="projeto_utilizador",
            password="projeto"
        )
        conn.autocommit = True
        cursor = conn.cursor()

        for table in table_list:
            query = f'DROP TABLE IF EXISTS "{table}" CASCADE;'
            print(f"A apagar tabela: {table}")
            cursor.execute(query)

        cursor.close()
        conn.close()
        print("Todas as tabelas foram apagadas com sucesso.")

    except Exception as e:
        print("Erro ao apagar tabelas:", e)


# 🔥 Lista de tabelas a apagar
tables_to_delete = [
    "dim_date",
    "dim_indicator",
    "dim_location",
    "dim_location_hierarchy",
    "fact_values",
    "dim_report"
]

drop_tables(tables_to_delete)