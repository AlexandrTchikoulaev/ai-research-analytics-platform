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


def run_pipeline():
    import bronze_validations
    import bronze
    import silver_validations
    import silver
    import gold_validations
    import gold
    import pipeline_data_report

    run_start = datetime.now()

    # Crash recovery: reset PROCESSING → PENDING (ficheiros que ficaram a meio num crash anterior)
    conn_reset = psycopg2.connect(**DB_CONFIG)
    cur_reset = conn_reset.cursor()
    cur_reset.execute("UPDATE op_data SET pipeline_status = 'PENDING' WHERE pipeline_status = 'PROCESSING'")
    conn_reset.commit()
    cur_reset.close(); conn_reset.close()

    print("\n PIPELINE DE DADOS INICIADO")
    print(f" {run_start.strftime('%Y-%m-%d %H:%M:%S')}\n")

    success = False
    try:
        # 1. Validar op_data
        run_step("1/6 — validate_opdata", bronze_validations.validate)

        # 2. Ingerir ficheiros brutos para Bronze
        run_step("2/6 — ingest_raw", bronze.main)

        # 3. Validar camada Bronze
        run_step("3/6 — validate_bronze", silver_validations.validate)

        # 4. Transformar para Silver
        run_step("4/6 — transform", silver.transformar)

        # 5. Validar camada Silver
        run_step("5/6 — validate_silver", gold_validations.validate)

        # 6. Carregar para o Data Warehouse
        run_step("6/6 — load", gold.run_etl)

        success = True
        print("\n PIPELINE DE DADOS CONCLUÍDO COM SUCESSO")

    finally:
        try:
            pipeline_data_report.generate(run_start, success)
        except Exception as e:
            print(f"[AVISO] Não foi possível gerar o relatório: {e}")


if __name__ == "__main__":
    run_pipeline()
