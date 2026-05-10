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

PROCESS_NAME = "etl_pdfs"


def update_timestamp():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            "UPDATE etl_data SET last_run = CURRENT_TIMESTAMP WHERE process_name = %s",
            (PROCESS_NAME,),
        )
        conn.commit()
        cur.close()
        print("Timestamp etl_pdfs atualizado.")
    except Exception as e:
        print(f"[AVISO] Falha ao atualizar timestamp: {e}")
    finally:
        if conn:
            conn.close()


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
    except Exception as e:
        print(f"[AVISO] Não foi possível obter last_run: {e}. A pipeline processará todos os registos.")
        return None


def run_pipeline():
    import validate_op_report
    import bronze
    import validate_bronze_unstructured
    import silver
    import report_pipeline_pdfs

    run_start     = datetime.now()
    prev_last_run = get_prev_last_run()

    print("\n PIPELINE DE PDFs INICIADO")
    print(f" {run_start.strftime('%Y-%m-%d %H:%M:%S')}\n")

    success = False
    try:
        valid_ids, _ = run_step("1/4 — validate_op_report", validate_op_report.validate)
        run_step("2/4 — bronze",                       lambda: bronze.main(valid_ids))
        run_step("3/4 — validate_bronze_unstructured", validate_bronze_unstructured.validate)
        run_step("4/4 — silver",                       silver.main)

        success = True
        print("\n PIPELINE DE PDFs CONCLUÍDO COM SUCESSO")

    finally:
        update_timestamp()
        try:
            report_pipeline_pdfs.generate(prev_last_run, run_start, success)
        except Exception as e:
            print(f"[AVISO] Não foi possível gerar o relatório: {e}")


if __name__ == "__main__":
    run_pipeline()
