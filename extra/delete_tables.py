import psycopg2

DATABASES = [
    {"host": "localhost", "port": "5433", "dbname": "warehouse_db",  "user": "projeto_utilizador", "password": "projeto"},
    {"host": "localhost", "port": "5433", "dbname": "operational_db","user": "projeto_utilizador", "password": "projeto"},
    {"host": "localhost", "port": "5433", "dbname": "pipeline_db",   "user": "projeto_utilizador", "password": "projeto"},
    {"host": "localhost", "port": "5433", "dbname": "vector_db",     "user": "projeto_utilizador", "password": "projeto"},
]

for cfg in DATABASES:
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()
    try:
        print(f"A apagar tabelas em {cfg['dbname']}...")
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public';
        """)
        tables = cur.fetchall()
        for (table_name,) in tables:
            print(f"  A apagar tabela: {table_name}")
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
        conn.commit()
        print(f"  {cfg['dbname']} limpa.")
    except Exception as e:
        conn.rollback()
        print(f"  Erro em {cfg['dbname']}:", e)
    finally:
        cur.close()
        conn.close()

print("Todas as tabelas foram apagadas!")
