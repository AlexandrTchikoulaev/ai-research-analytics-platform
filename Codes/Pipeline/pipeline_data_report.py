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
        # Normalizar file_ids para strings (file_id é character varying na BD)
        if run_file_ids:
            run_file_ids = [str(fid) for fid in run_file_ids]

        # ── Candidatos desta run ──────────────────────────────────────────────
        if run_file_ids:
            cur_op.execute("""
                SELECT d.file_id, d.report_id, d.file_url,
                       d.pipeline_status, d.pipeline_error,
                       d.auto_generate,
                       d.transform_fn_name, d.transform_fn_source,
                       r.source_code
                FROM op_data d
                LEFT JOIN op_report r ON r.report_id = d.report_id
                WHERE d.file_id = ANY(%s)
                ORDER BY d.file_id
            """, (run_file_ids,))
        else:
            cur_op.execute("""
                SELECT d.file_id, d.report_id, d.file_url,
                       d.pipeline_status, d.pipeline_error,
                       d.auto_generate,
                       d.transform_fn_name, d.transform_fn_source,
                       r.source_code
                FROM op_data d
                LEFT JOIN op_report r ON r.report_id = d.report_id
                WHERE d.pipeline_status != 'PENDING'
                ORDER BY d.file_id
            """)
        candidates = cur_op.fetchall()

        # ── Logs desta run (file_ids desta run + após run_start) ──────────────
        cand_file_ids = [r["file_id"] for r in candidates]
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

        invalid_opdata = errs("validate_opdata")
        ingest_errs    = errs("ingest_raw")
        bronze_errs    = errs("validate_bronze")
        transform_errs = errs("transform")
        load_errs_list = by_step.get("load", [])

        status_map = {r["file_id"]: r["pipeline_status"] for r in candidates}
        error_map  = {r["file_id"]: r["pipeline_error"]  for r in candidates}

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
        s1_total   = len(candidates)
        s1_invalid = len(invalid_opdata)
        s1_valid   = s1_total - s1_invalid

        valid_cands = [r for r in candidates if r["file_id"] not in invalid_opdata]
        s2_url      = sum(1 for r in valid_cands if r["file_url"])
        s2_upload   = sum(1 for r in valid_cands if not r["file_url"])
        s2_url_ok   = s2_url - len(ingest_errs)
        s2_url_err  = len(ingest_errs)
        s2_ok       = s2_url_ok + s2_upload

        s3_invalid = len(bronze_errs)
        s3_valid   = s2_ok - s3_invalid

        s4_ok  = sum(1 for r in candidates if r["pipeline_status"] in ("SILVER_OK", "DONE"))
        s4_err = sum(1 for r in candidates
                     if r["pipeline_status"] == "FAILED"
                     and r["file_id"] not in invalid_opdata
                     and r["file_id"] not in ingest_errs
                     and r["file_id"] not in bronze_errs)

        s5_invalid = len(errs("validate_silver"))
        s5_valid   = s4_ok - s5_invalid

        fn_gerada = sum(1 for r in candidates if r["transform_fn_source"] == "ai_gerada")
        fn_cache  = sum(1 for r in candidates if r["transform_fn_source"] == "ai_cache")
        fn_manual = sum(1 for r in candidates if r["transform_fn_source"] == "manual")

        s6_done = sum(1 for r in candidates if r["pipeline_status"] == "DONE")
        s6_errs = len(load_errs_list)

        # ══════════════════════════════════════════════════════════════════════
        # CABEÇALHO
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("RELATÓRIO DA PIPELINE DE DADOS")
        w(f"Gerado em          : {now_str}")
        w(f"Início da execução : {start_str}")
        w(f"Ficheiros nesta run: {s1_total}")
        w(f"Estado             : {'CONCLUÍDA COM SUCESSO' if success else 'FALHOU'}")
        w(SEP)
        w("")

        w(DASH)
        w("RESUMO")
        w(DASH)
        w(f"[1] validate_opdata      Total: {s1_total:<4}  Válidos: {s1_valid:<4}  Inválidos: {s1_invalid:<4}  → etl_logs: {s1_invalid}")
        w(f"[2] Ingestão Bronze      Processados: {s1_valid:<4}  OK: {s2_ok:<4}  Erro: {s2_url_err:<4}  → etl_logs: {s2_url_err}")
        w(f"[3] Validação Bronze     Validados: {s2_ok:<4}  OK: {s3_valid:<4}  Inválidos: {s3_invalid:<4}  → etl_logs: {s3_invalid}")
        fn_detail = f"  [AI gerada: {fn_gerada}  AI cache: {fn_cache}  manual: {fn_manual}]"
        w(f"[4] Transformação Silver OK (SILVER_OK/DONE): {s4_ok:<4}  Erro: {s4_err:<4}{fn_detail}  → etl_logs: {len(transform_errs)}")
        w(f"[5] Validação Silver     OK: {s5_valid:<4}  Inválidos: {s5_invalid:<4}  → etl_logs: {s5_invalid}")
        w(f"[6] Warehouse (gold)     DONE: {s6_done:<4}  dim_report: {dim_report_run}  dim_location: {dim_location_total}  "
          f"dim_indicator: {dim_indicator_total}  fact_values: {total_facts}  Erros: {s6_errs}")
        w("")

        # ══════════════════════════════════════════════════════════════════════
        # PASSO 1 — validate_opdata
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("1/6 — VALIDACAO op_data")
        w(SEP)
        if not candidates:
            w("  Sem ficheiros para validar.")
        else:
            for r in candidates:
                fid   = r["file_id"]
                label = f"[{r['source_code']}]" if r["source_code"] else f"report_id={r['report_id']}"
                mode  = "manual" if not r["auto_generate"] else "auto"
                if fid in invalid_opdata:
                    w(f"  INVALIDO  file_id={fid}  {label}  mode={mode}")
                    w(f"            Erro: {invalid_opdata[fid]}")
                else:
                    w(f"  OK        file_id={fid}  {label}  mode={mode}")
        w("")

        # ══════════════════════════════════════════════════════════════════════
        # PASSO 2 — ingest_raw (Bronze)
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("2/6 — INGESTAO Bronze (ingest_raw)")
        w(SEP)
        if not candidates:
            w("  Sem ficheiros para ingerir.")
        else:
            for r in candidates:
                fid = r["file_id"]
                url = r["file_url"] or ""
                if fid in invalid_opdata:
                    w(f"  IGNORADO  file_id={fid}  [invalidado em validate_opdata]")
                elif not url:
                    w(f"  UPLOAD    file_id={fid}  [ficheiro carregado via upload — ja no MinIO]")
                elif fid in ingest_errs:
                    w(f"  ERRO      file_id={fid}  URL={url}")
                    w(f"            Erro: {ingest_errs[fid]}")
                else:
                    w(f"  OK        file_id={fid}  URL={url}")
        w("")

        # ══════════════════════════════════════════════════════════════════════
        # PASSO 3 — validate_bronze
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("3/6 — VALIDACAO Bronze (validate_bronze)")
        w(SEP)
        checked = [r for r in candidates
                   if r["file_id"] not in invalid_opdata
                   and r["file_id"] not in ingest_errs]
        if not checked:
            w("  Nenhum objecto Bronze para validar.")
        else:
            for r in checked:
                fid = r["file_id"]
                if fid in bronze_errs:
                    w(f"  INVALIDO  file_id={fid}")
                    w(f"            Erro: {bronze_errs[fid]}")
                else:
                    w(f"  OK        file_id={fid}")
        w("")

        # ══════════════════════════════════════════════════════════════════════
        # PASSO 4 — transform (Silver)
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("4/6 — TRANSFORMACAO Silver (transform)")
        w(SEP)
        to_transform = [r for r in candidates
                        if r["file_id"] not in invalid_opdata
                        and r["file_id"] not in ingest_errs
                        and r["file_id"] not in bronze_errs]
        if not to_transform:
            w("  Nenhum ficheiro chegou a esta fase.")
        else:
            _fn_src_label = {
                "ai_gerada": "[AI gerada]",
                "ai_cache":  "[AI cache]",
                "manual":    "[manual]",
            }
            for r in to_transform:
                fid      = r["file_id"]
                fn_name  = r["transform_fn_name"] or "—"
                fn_src   = r["transform_fn_source"] or ""
                fn_label = _fn_src_label.get(fn_src, f"[{fn_src}]") if fn_src else ""
                fn_str   = f"fn={fn_name} {fn_label}".strip()
                status   = status_map.get(fid, "PENDING")
                if fid in transform_errs:
                    w(f"  ERRO      file_id={fid}  {fn_str}")
                    w(f"            Erro: {transform_errs[fid]}")
                elif status in ("SILVER_OK", "DONE"):
                    w(f"  OK        file_id={fid}  {fn_str}  -> {fid}.parquet")
                elif status == "FAILED":
                    w(f"  FAILED    file_id={fid}  {fn_str}")
                    w(f"            Erro: {error_map.get(fid) or '—'}")
                else:
                    w(f"  {status:<9} file_id={fid}  {fn_str}")
        w("")

        # ══════════════════════════════════════════════════════════════════════
        # PASSO 5 — validate_silver
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("5/6 — VALIDACAO Silver (validate_silver)")
        w(SEP)
        silver_errs_map = errs("validate_silver")
        to_validate_silver = [r for r in to_transform
                              if status_map.get(r["file_id"], "PENDING") in ("SILVER_OK", "DONE")
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
        # PASSO 6 — gold (load)
        # ══════════════════════════════════════════════════════════════════════
        w(SEP)
        w("6/6 — CARREGAMENTO Warehouse (gold)")
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
