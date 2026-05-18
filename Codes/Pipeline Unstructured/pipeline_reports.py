"""
Orquestrador do pipeline de dados não estruturados (PDFs).
Executa sequencialmente:
  1. validate_op_report           — valida registos em op_report antes de ingerir
  2. bronze                       — descarrega PDFs para o bucket bronze-unstructured
  3. validate_bronze_unstructured — valida PDFs no bucket bronze-unstructured
  4. silver                       — processa PDFs e indexa embeddings na BD vetorial
"""
import sys
import os
import psycopg2
from datetime import datetime

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
    import bronze_validations as validate_op_report
    import bronze
    import silver_validations as validate_bronze_unstructured
    import silver
    import pipeline_reports_report as report_pipeline_pdfs

    run_start = datetime.now()

    # Crash recovery: reset PROCESSING → PENDING (relatórios que ficaram a meio num crash anterior)
    conn_reset = psycopg2.connect(**DB_CONFIG)
    cur_reset  = conn_reset.cursor()
    cur_reset.execute("UPDATE op_report SET pipeline_status = 'PENDING' WHERE pipeline_status = 'PROCESSING'")
    conn_reset.commit()
    cur_reset.close(); conn_reset.close()

    print("\n PIPELINE DE PDFs INICIADO")
    print(f" {run_start.strftime('%Y-%m-%d %H:%M:%S')}\n")

    success = False
    try:
        run_step("1/4 — validate_op_report",        validate_op_report.validate)
        run_step("2/4 — bronze",                     bronze.main)
        run_step("3/4 — validate_bronze_unstructured", validate_bronze_unstructured.validate)
        run_step("4/4 — silver",                     silver.main)

        success = True
        print("\n PIPELINE DE PDFs CONCLUÍDO COM SUCESSO")

    finally:
        try:
            report_pipeline_pdfs.generate(run_start, success)
        except Exception as e:
            print(f"[AVISO] Não foi possível gerar o relatório: {e}")


if __name__ == "__main__":
    run_pipeline()
