import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname="pipeline_db",
    user="projeto_utilizador",
    password="projeto"
)

cur = conn.cursor()

cur.execute(
    "UPDATE etl_data SET last_run = '2000-01-01 00:00:00' WHERE process_name IN ('etl_dados', 'etl_pdfs');"
)

conn.commit()

cur.close()
conn.close()

print("Timestamps de etl_dados e etl_pdfs atualizados para 2000-01-01")