import psycopg2
import os

# ligação à base de dados
conn = psycopg2.connect(
    host="localhost",
    port="5433",  # ajusta se for diferente
    dbname="operational_db",
    user="projeto_utilizador",
    password="projeto"
)

cur = conn.cursor()

# caminhos dos ficheiros relativos à localização do script
_base = os.path.dirname(os.path.abspath(__file__))
op_report_path = os.path.join(_base, "csv", "op_report.csv")
op_data_path   = os.path.join(_base, "csv", "op_data.csv")


try:
    # limpar tabelas (forma correta)
    cur.execute("TRUNCATE TABLE op_data, op_report;")

    # carregar op_report
    with open(op_report_path, 'r', encoding='utf-8') as f:
        cur.copy_expert(
            """
            COPY op_report (
                report_id,
                file_name,
                source_code,
                report_url,
                publication_date,
                area_tematica,
                estado,
                palavras_chave,
                resumo
            )
            FROM STDIN WITH CSV HEADER DELIMITER ','
            """,
            f
        )

    # carregar op_data
    with open(op_data_path, 'r', encoding='utf-8') as f:
        cur.copy_expert(
        """
        COPY op_data (
            file_id,
            report_id,
            file_name,
            file_url,
            extract_function,
            file_type
        )
        FROM STDIN WITH CSV HEADER DELIMITER ','
        """,
        f
    )

    conn.commit()
    print(" Dados inseridos com sucesso!")

except Exception as e:
    conn.rollback()
    print(" Erro:", e)

finally:
    cur.close()
    conn.close()