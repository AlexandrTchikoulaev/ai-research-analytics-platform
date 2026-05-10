"""
Orquestrador do pipeline de dados estruturados e semi-estruturados.
Executa sequencialmente:
  1. validate_opdata  — valida registos em op_data antes de ingerir
  2. ingest_raw       — descarrega ficheiros para o bucket Bronze
  3. validate_bronze  — valida objetos no bucket Bronze
  4. transform        — transforma para Parquet no bucket Silver
  5. validate_silver  — valida estrutura dos Parquets
  6. load             — carrega para o Data Warehouse
"""
import sys
import os
import psycopg2
from datetime import datetime

# Permitir imports diretos dos módulos na mesma pasta
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "gestao_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

PROCESS_NAME = "etl_dados"


def update_timestamp():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        UPDATE etl_data SET last_run = CURRENT_TIMESTAMP
        WHERE process_name = %s
    """, (PROCESS_NAME,))
    conn.commit()
    cur.close()
    conn.close()
    print("Timestamp etl_dados atualizado.")


def run_step(label: str, fn):
    print(f"\n{'='*50}")
    print(f" {label}")
    print(f"{'='*50}")
    try:
        result = fn()
        print(f"[OK] {label} concluído.")
        return result
    except Exception as e:
        print(f"[ERRO] {label} falhou: {e}")
        raise


def get_prev_last_run():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT last_run FROM etl_data WHERE process_name = %s", (PROCESS_NAME,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def run_pipeline():
    import validate_opdata
    import bronze
    import validate_bronze
    import silver
    import validate_silver
    import gold
    import report_pipeline

    run_start    = datetime.now()
    prev_last_run = get_prev_last_run()

    print("\n PIPELINE DE DADOS INICIADO")
    print(f" {run_start.strftime('%Y-%m-%d %H:%M:%S')}\n")

    success = False
    try:
        # 1. Validar op_data
        run_step("1/6 — validate_opdata", validate_opdata.validate)

        # 2. Ingerir ficheiros brutos para Bronze
        run_step("2/6 — ingest_raw", bronze.main)

        # 3. Validar camada Bronze
        run_step("3/6 — validate_bronze", validate_bronze.validate)

        # 4. Transformar para Silver
        run_step("4/6 — transform", silver.transformar)

        # 5. Validar camada Silver
        run_step("5/6 — validate_silver", validate_silver.validate)

        # 6. Carregar para o Data Warehouse
        run_step("6/6 — load", gold.run_etl)

        success = True
        print("\n PIPELINE DE DADOS CONCLUÍDO COM SUCESSO")

    finally:
        update_timestamp()
        try:
            report_pipeline.generate(prev_last_run, run_start, success)
        except Exception as e:
            print(f"[AVISO] Não foi possível gerar o relatório: {e}")


if __name__ == "__main__":
    run_pipeline()
