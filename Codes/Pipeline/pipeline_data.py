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
import traceback
import psycopg2
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DB_CONFIG

PIPELINE_LOCK_ID = 987654321


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

    # Impede execuções simultâneas — o lock é libertado automaticamente ao fechar a ligação
    conn_lock = psycopg2.connect(**DB_CONFIG)
    cur_lock = conn_lock.cursor()
    cur_lock.execute("SELECT pg_try_advisory_lock(%s)", (PIPELINE_LOCK_ID,))
    locked = cur_lock.fetchone()[0]
    if not locked:
        print("[AVISO] Outra instância do pipeline já está em execução. A abortar.")
        cur_lock.close()
        conn_lock.close()
        return

    # Crash recovery: reset PROCESSING/VALIDATED → PENDING
    conn_reset = psycopg2.connect(**DB_CONFIG)
    cur_reset = conn_reset.cursor()
    cur_reset.execute("""
        UPDATE op_data
        SET pipeline_status = 'PENDING'
        WHERE pipeline_status IN ('PROCESSING', 'VALIDATED')
    """)
    conn_reset.commit()

    # Capturar file_ids PENDING agora — define o escopo do relatório desta run
    cur_reset.execute("SELECT file_id FROM op_data WHERE pipeline_status = 'PENDING'")
    run_file_ids = [r[0] for r in cur_reset.fetchall()]
    cur_reset.close()
    conn_reset.close()

    print("\n PIPELINE DE DADOS INICIADO")
    print(f" {run_start.strftime('%Y-%m-%d %H:%M:%S')}\n")

    success = False
    try:
        run_step("1/6 — validate_opdata", bronze_validations.validate)
        run_step("2/6 — ingest_raw", bronze.main)
        run_step("3/6 — validate_bronze", silver_validations.validate)
        run_step("4/6 — transform", silver.transformar)
        run_step("5/6 — validate_silver", gold_validations.validate)
        run_step("6/6 — load", gold.run_etl)

        success = True
        print("\n PIPELINE DE DADOS CONCLUÍDO COM SUCESSO")

    finally:
        try:
            pipeline_data_report.generate(run_start, success, run_file_ids)
        except Exception as e:
            print(f"[AVISO] Não foi possível gerar o relatório: {e}")
            try:
                _err_log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline_report_error.log")
                with open(_err_log, "a", encoding="utf-8") as _f:
                    _f.write(f"\n{datetime.now().isoformat()} — ERRO ao gerar relatório: {e}\n")
                    _f.write(traceback.format_exc())
                print(f"[AVISO] Detalhe do erro guardado em: {_err_log}")
            except Exception:
                pass

        cur_lock.close()
        conn_lock.close()


if __name__ == "__main__":
    run_pipeline()
