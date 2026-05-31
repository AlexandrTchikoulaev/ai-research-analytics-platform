"""
Orquestrador do pipeline de dados não estruturados (PDFs).
Executa sequencialmente:
  1. bronze — descarrega PDFs para o bucket bronze-unstructured
  2. silver — processa PDFs e indexa embeddings na BD vetorial
"""
import sys
import os
import time
import json
import psycopg2
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_STEP_FILE = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "Reports", "PDFs", "meta", "pipeline_pdfs_status.json",
))


def _set_step(step: str):
    try:
        os.makedirs(os.path.dirname(_STEP_FILE), exist_ok=True)
        with open(_STEP_FILE, "w", encoding="utf-8") as f:
            json.dump({"step": step}, f)
    except Exception:
        pass


def _try_report(run_start):
    """Escreve/atualiza o relatório com estado 'A EXECUTAR...' — silencioso se falhar."""
    try:
        report_pipeline_pdfs.generate(run_start, None)
    except Exception:
        pass

import bronze
import silver
import pipeline_reports_report as report_pipeline_pdfs

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
    t0 = time.monotonic()
    try:
        result = fn()
        elapsed = time.monotonic() - t0
        print(f"[OK] {label} concluído em {elapsed:.1f}s.")
        return result
    except Exception as e:
        elapsed = time.monotonic() - t0
        print(f"[ERRO] {label} falhou após {elapsed:.1f}s: {e}")
        raise


def run_pipeline():
    run_start = datetime.now()
    success   = False

    try:
        # Crash recovery + migração de status UPPERCASE → lowercase
        try:
            conn_reset = psycopg2.connect(**DB_CONFIG)
            try:
                cur_reset = conn_reset.cursor()
                cur_reset.execute("UPDATE op_report SET pipeline_status = 'pending'    WHERE pipeline_status IN ('PENDING', 'VALIDATED')")
                cur_reset.execute("UPDATE op_report SET pipeline_status = 'pending'    WHERE pipeline_status = 'PROCESSING'")
                cur_reset.execute("UPDATE op_report SET pipeline_status = 'pending'    WHERE pipeline_status = 'BRONZE_OK'")
                cur_reset.execute("UPDATE op_report SET pipeline_status = 'done'       WHERE pipeline_status = 'DONE'")
                cur_reset.execute("UPDATE op_report SET pipeline_status = 'failed'     WHERE pipeline_status = 'FAILED'")
                cur_reset.execute("UPDATE op_report SET pipeline_status = 'pending'    WHERE pipeline_status = 'processing'")
                conn_reset.commit()
                cur_reset.close()
            finally:
                conn_reset.close()
        except Exception as e:
            print(f"[AVISO] Crash recovery falhou (Postgres em baixo?): {e}")

        print("\n PIPELINE DE PDFs INICIADO")
        print(f" {run_start.strftime('%Y-%m-%d %H:%M:%S')}\n")

        report_pipeline_pdfs.write_initial(run_start)  # ficheiro criado imediatamente

        _set_step("bronze")
        run_step("1/2 — bronze", bronze.main)
        _try_report(run_start)  # relatório parcial após bronze (PDFs armazenados)

        _set_step("silver")
        run_step("2/2 — silver", silver.main)

        success = True
        elapsed = (datetime.now() - run_start).total_seconds()
        print(f"\n PIPELINE DE PDFs CONCLUÍDO COM SUCESSO (duração total: {elapsed:.1f}s)")

    finally:
        _set_step("idle")
        try:
            report_pipeline_pdfs.generate(run_start, success)  # relatório final
        except Exception as e:
            print(f"[AVISO] Não foi possível gerar o relatório: {e}")


if __name__ == "__main__":
    run_pipeline()
