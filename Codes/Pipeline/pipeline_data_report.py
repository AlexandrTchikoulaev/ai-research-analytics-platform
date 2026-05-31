"""
Gerador de relatório detalhado da última execução da pipeline de dados.
Chamado pelo pipeline_data.py no final de cada execução.
"""
import os
import psycopg2
import psycopg2.extras
from datetime import datetime
from config import DB_CONFIG, DB_WAREHOUSE

REPORTS_DIR = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "Reports", "Data"
))

SEP  = "=" * 72
DASH = "-" * 72


def generate(run_start: datetime, success: bool, run_file_ids=None):
    """
    run_file_ids: lista de file_ids que estavam PENDING no início desta run.
                  Limita o relatório aos ficheiros processados nesta execução.
                  Se None, mostra todos os ficheiros não-PENDING (fallback legacy).
    """
    lines = []
    w = lines.append

    now_str   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_str = run_start.strftime("%Y-%m-%d %H:%M:%S")

    conn_op = psycopg2.connect(**DB_CONFIG)
    cur_op  = conn_op.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    conn_dw = psycopg2.connect(**DB_WAREHOUSE)
    cur_dw  = conn_dw.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # ── Candidatos desta run ──────────────────────────────────────────────
        if run_file_ids:
            cur_op.execute("""
                SELECT d.file_id, d.report_id, d.file_url,
                       d.pipeline_status,
                       r.source_code
                FROM op_data d
                LEFT JOIN op_report r ON r.report_id = d.report_id
                WHERE d.file_id = ANY(%s)
                ORDER BY d.file_id
            """, (list(run_file_ids),))
        else:
            cur_op.execute("""
                SELECT d.file_id, d.report_id, d.file_url,
                       d.pipeline_status,
                       r.source_code
                FROM op_data d
                LEFT JOIN op_report r ON r.report_id = d.report_id
                WHERE d.pipeline_status != 'pending'
                ORDER BY d.file_id
            """)
        candidates = cur_op.fetchall()

        # ── Logs desta run (file_ids desta run + após run_start) ──────────────
        # etl_logs_dados.file_id é VARCHAR; op_data.file_id é INTEGER — forçar strings
        cand_file_ids = [str(r["file_id"]) for r in candidates]
        if cand_file_ids:
            cur_op.execute("""
                SELECT file_id, step, error_message, log_time
                FROM etl_logs_dados
                WHERE file_id = ANY(%s)
                  AND log_time >= %s
                ORDER BY log_time ASC
            """, (cand_file_ids, run_start))
        else:
            cur_op.execute("SELECT file_id, step, error_message, log_time FROM etl_logs_dados WHERE FALSE")
        run_logs = cur_op.fetchall()

        by_step = {}
        for lg in run_logs:
            by_step.setdefault(lg["step"], []).append(lg)

        def errs(step):
            return {lg["file_id"]: lg["error_message"] for lg in by_step.get(step, [])}

        ingest_errs    = errs("bronze")
        transform_errs = errs("silver")
        load_errs_list = by_step.get("gold", [])

        status_map = {r["file_id"]: r["pipeline_status"] for r in candidates}

        # ── fact_values + dimensões ───────────────────────────────────────────
        rids = list({r["report_id"] for r in candidates if r["report_id"]})

        try:
            if rids:
                cur_dw.execute("SELECT COUNT(*) AS cnt FROM fact_values WHERE report_id = ANY(%s)", (rids,))
                total_facts = cur_dw.fetchone()["cnt"]
            else:
                total_facts = 0
        except Exception:
            total_facts = "—"

        try:
            cur_dw.execute("SELECT COUNT(*) AS cnt FROM fact_values")
            total_facts_dw = cur_dw.fetchone()["cnt"]
        except Exception:
            total_facts_dw = "—"

        try:
            if rids:
                cur_dw.execute("SELECT COUNT(*) AS cnt FROM dim_report WHERE report_id = ANY(%s)", (rids,))
                dim_report_run = cur_dw.fetchone()["cnt"]
            else:
                dim_report_run = 0
            cur_dw.execute("SELECT COUNT(*) AS cnt FROM dim_report")
            dim_report_total = cur_dw.fetchone()["cnt"]
        except Exception:
            dim_report_run = "—"
            dim_report_total = "—"

        try:
            cur_dw.execute("SELECT COUNT(*) AS cnt FROM dim_location")
            dim_location_total = cur_dw.fetchone()["cnt"]
        except Exception:
            dim_location_total = "—"

        try:
            cur_dw.execute("SELECT COUNT(*) AS cnt FROM dim_indicator")
            dim_indicator_total = cur_dw.fetchone()["cnt"]
        except Exception:
            dim_indicator_total = "—"

        # ── Contagens para o RESUMO ───────────────────────────────────────────
        s1_total  = len(candidates)
        s1_url    = sum(1 for r in candidates if r["file_url"])
        s1_upload = sum(1 for r in candidates if not r["file_url"])
        s1_ok     = s1_total - len(ingest_errs)
        s1_err    = len(ingest_errs)

        s2_ok  = sum(1 for r in candidates if r["pipeline_status"] in ("silver", "done"))
        s2_err = sum(1 for r in candidates
                     if r["pipeline_status"] == "failed"
                     and r["file_id"] not in ingest_errs)

        s3_invalid = len(errs("gold_validations"))
        s3_valid   = s2_ok - s3_invalid

        s4_done = sum(1 for r in candidates if r["pipeline_status"] == "done")
        s4_errs = len(load_errs_list)

        # ══════════════════════════════════════════════════════════════════════
        # CABEÇALHO
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("RELATÓRIO DA PIPELINE DE DADOS")
        w(f"Gerado em          : {now_str}")
        w(f"Início da execução : {start_str}")
        w(f"Ficheiros nesta run : {s1_total}  (link: {s1_url}  upload: {s1_upload})")
        w(f"Estado             : {'CONCLUÍDA COM SUCESSO' if success else 'FALHOU'}")
        w(SEP)
        w("")

        w(DASH)
        w("RESUMO")
        w(DASH)
        w(f"[1] bronze               Total: {s1_total:<4}  OK: {s1_ok:<4}  Erro: {s1_err:<4}  → etl_logs: {s1_err}")
        w(f"[2] silver               OK (SILVER/DONE): {s2_ok:<4}  Erro: {s2_err:<4}  → etl_logs: {len(transform_errs)}")
        w(f"[3] gold_validations     OK: {s3_valid:<4}  Inválidos: {s3_invalid:<4}  → etl_logs: {s3_invalid}")
        w(f"[4] gold                 DONE: {s4_done:<4}  dim_report: {dim_report_run}  dim_location: {dim_location_total}  "
          f"dim_indicator: {dim_indicator_total}  fact_values: {total_facts}  Erros: {s4_errs}")
        w("")

        # ══════════════════════════════════════════════════════════════════════
        # PASSO 1 — bronze
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("1/4 — bronze")
        w(SEP)
        if not candidates:
            w("  Sem ficheiros para ingerir.")
        else:
            for r in candidates:
                fid = r["file_id"]
                url = r["file_url"] or ""
                if not url:
                    w(f"  UPLOAD    file_id={fid}  [ficheiro carregado via upload — ja no MinIO]")
                elif fid in ingest_errs:
                    w(f"  ERRO      file_id={fid}  URL={url}")
                    w(f"            Erro: {ingest_errs[fid]}")
                else:
                    w(f"  OK        file_id={fid}  URL={url}")
        w("")

        # ══════════════════════════════════════════════════════════════════════
        # PASSO 2 — silver
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("2/4 — silver")
        w(SEP)
        to_transform = [r for r in candidates if r["file_id"] not in ingest_errs]
        if not to_transform:
            w("  Nenhum ficheiro chegou a esta fase.")
        else:
            for r in to_transform:
                fid    = r["file_id"]
                status = status_map.get(fid, "pending")
                if fid in transform_errs:
                    w(f"  ERRO      file_id={fid}")
                    w(f"            Erro: {transform_errs[fid]}")
                elif status in ("silver", "done"):
                    w(f"  OK        file_id={fid}  -> {fid}.parquet")
                elif status == "failed":
                    w(f"  FAILED    file_id={fid}")
                    w(f"            Erro: {transform_errs.get(fid) or '—'}")
                else:
                    w(f"  {status:<9} file_id={fid}")
        w("")

        # ══════════════════════════════════════════════════════════════════════
        # PASSO 3 — gold_validations
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("3/4 — gold_validations")
        w(SEP)
        silver_errs_map = errs("gold_validations")
        to_validate_silver = [r for r in to_transform
                              if status_map.get(r["file_id"], "pending") in ("silver", "done")
                              and r["file_id"] not in transform_errs]
        if not to_validate_silver:
            w("  Nenhum ficheiro chegou a esta fase.")
        else:
            for r in to_validate_silver:
                fid = r["file_id"]
                if fid in silver_errs_map:
                    w(f"  INVALIDO  file_id={fid}")
                    w(f"            Erro: {silver_errs_map[fid]}")
                else:
                    w(f"  OK        file_id={fid}")
        w("")

        # ══════════════════════════════════════════════════════════════════════
        # PASSO 4 — gold
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("4/4 — gold")
        w(SEP)
        if not load_errs_list:
            w("  Sem erros de carregamento registados.")
        else:
            for lg in load_errs_list:
                w(f"  ERRO      step={lg['step']}  file_id={lg['file_id'] or '—'}")
                w(f"            Erro: {lg['error_message']}")
        w("")
        w("  ESTADO DO DATA WAREHOUSE (após esta run):")
        w(f"  {'dim_report':<24} desta run : {str(dim_report_run):<8}  total DW : {dim_report_total}")
        w(f"  {'dim_location':<24} total DW  : {dim_location_total}")
        w(f"  {'dim_indicator':<24} total DW  : {dim_indicator_total}")
        w(f"  {'fact_values':<24} desta run : {str(total_facts):<8}  total DW : {total_facts_dw}")
        w("")

        # ══════════════════════════════════════════════════════════════════════
        # TODOS OS ERROS ETL_LOGS DESTA EXECUCAO
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("REGISTOS etl_logs_dados DESTA EXECUCAO")
        w(SEP)
        if not run_logs:
            w("  Nenhum erro registado.")
        else:
            header = f"  {'file_id':<10} {'step':<22} {'log_time':<22} {'erro'}"
            w(header)
            w("  " + "-" * 70)
            for lg in run_logs:
                fid  = str(lg["file_id"]  or "—")
                step = str(lg["step"]     or "—")
                lt   = str(lg["log_time"])[:19]
                msg  = (lg["error_message"] or "")
                w(f"  {fid:<10} {step:<22} {lt:<22} {msg}")
        w("")

        w(SEP)
        w("FIM DO RELATORIO")
        w(SEP)

    finally:
        cur_op.close(); conn_op.close()
        cur_dw.close(); conn_dw.close()

    timestamp = run_start.strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(REPORTS_DIR, f"d_{timestamp}.txt")
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Relatorio guardado em: {report_path}")
    return report_path
