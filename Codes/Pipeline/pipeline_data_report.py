"""
Gerador de relatório detalhado da última execução da pipeline de dados.
Chamado pelo pipeline_dados.py no final de cada execução.
"""
import os
import psycopg2
import psycopg2.extras
import boto3
from datetime import datetime

DB_PIPELINE = {
    "host": "localhost", "port": 5433, "dbname": "gestao_db",
    "user": "projeto_utilizador", "password": "projeto",
}
DB_OPERATIONAL = {
    "host": "localhost", "port": 5433, "dbname": "gestao_db",
    "user": "projeto_utilizador", "password": "projeto",
}
DB_WAREHOUSE = {
    "host": "localhost", "port": 5433, "dbname": "warehouse_db",
    "user": "projeto_utilizador", "password": "projeto",
}
MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

REPORT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "Reports",
    "pipeline_data_report.txt"
)

SEP  = "=" * 72
DASH = "-" * 72


def _minio_keys(s3, bucket):
    keys = set()
    try:
        pag = s3.get_paginator("list_objects_v2")
        for page in pag.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                keys.add(obj["Key"])
    except Exception:
        pass
    return keys


def generate(prev_last_run, run_start: datetime, success: bool):
    lines = []
    w = lines.append

    now_str   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_str = run_start.strftime("%Y-%m-%d %H:%M:%S")
    prev_str  = prev_last_run.strftime("%Y-%m-%d %H:%M:%S") if prev_last_run else "nunca executado"

    # ── Conexões ──────────────────────────────────────────────────────────
    conn_pipe = psycopg2.connect(**DB_PIPELINE)
    cur_pipe  = conn_pipe.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    conn_op   = psycopg2.connect(**DB_OPERATIONAL)
    cur_op    = conn_op.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    conn_dw   = psycopg2.connect(**DB_WAREHOUSE)
    cur_dw    = conn_dw.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        s3 = boto3.client("s3", **MINIO_CONFIG)
    except Exception:
        s3 = None

    # ── Candidatos ────────────────────────────────────────────────────────
    if prev_last_run:
        cur_op.execute("""
            SELECT d.file_id, d.report_id, d.file_url, d.extract_function,
                   d.created_at,
                   r.file_name AS report_name, r.source_code
            FROM op_data d
            LEFT JOIN op_report r ON r.report_id = d.report_id
            WHERE d.created_at > %s
            ORDER BY d.file_id
        """, (prev_last_run,))
    else:
        cur_op.execute("""
            SELECT d.file_id, d.report_id, d.file_url, d.extract_function,
                   d.created_at,
                   r.file_name AS report_name, r.source_code
            FROM op_data d
            LEFT JOIN op_report r ON r.report_id = d.report_id
            ORDER BY d.file_id
        """)
    candidates = cur_op.fetchall()

    # ── Logs desta execução ────────────────────────────────────────────────
    # Usa prev_last_run (timestamp do PostgreSQL, UTC) como lower bound para
    # evitar desfasamento com datetime.now() (hora local). Na primeira run
    # (prev_last_run=None) mostra todos os logs.
    if prev_last_run is not None:
        cur_pipe.execute("""
            SELECT file_id, file_name, step, error_message, log_time
            FROM etl_logs_dados
            WHERE log_time > %s
            ORDER BY log_time ASC
        """, (prev_last_run,))
    else:
        cur_pipe.execute("""
            SELECT file_id, file_name, step, error_message, log_time
            FROM etl_logs_dados
            ORDER BY log_time ASC
        """)
    run_logs = cur_pipe.fetchall()

    # Organizar por step e por file_id
    by_step = {}
    for lg in run_logs:
        by_step.setdefault(lg["step"], []).append(lg)

    def _norm_fid(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return v

    def errs(step):
        return {_norm_fid(lg["file_id"]): lg["error_message"] for lg in by_step.get(step, [])}

    invalid_opdata  = errs("validate_opdata")
    ingest_errs     = errs("ingest_raw")
    bronze_errs     = errs("validate_bronze")
    transform_errs  = errs("transform")
    silver_errs     = errs("validate_silver")
    load_errs_list  = by_step.get("load", []) + by_step.get("load_mapping", [])

    candidate_ids = {r["file_id"] for r in candidates}

    # ── MinIO ─────────────────────────────────────────────────────────────
    bronze_keys = _minio_keys(s3, "bronze") if s3 else set()
    silver_keys = _minio_keys(s3, "silver") if s3 else set()

    # ── fact_values + dimensões ────────────────────────────────────────────
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

    # ══════════════════════════════════════════════════════════════════════
    # CABEÇALHO
    # ══════════════════════════════════════════════════════════════════════
    w(SEP)
    w("RELATÓRIO DA PIPELINE DE DADOS")
    w(f"Gerado em          : {now_str}")
    w(f"Início da execução : {start_str}")
    w(f"Execução anterior  : {prev_str}")
    w(f"Estado             : {'CONCLUÍDA COM SUCESSO' if success else 'FALHOU'}")
    w(SEP)
    w("")

    # ── Contagens derivadas para o RESUMO ─────────────────────────────────
    # Ficheiros cujo parquet não existe no Silver (erro silencioso ou timezone)
    transform_missing = {
        r["file_id"] for r in candidates
        if r["file_id"] not in invalid_opdata
        and r["file_id"] not in ingest_errs
        and r["file_id"] not in bronze_errs
        and r["file_id"] not in transform_errs
        and f"{r['file_id']}.parquet" not in silver_keys
    }

    # [1] validate_opdata
    s1_total    = len(candidates)
    s1_invalid  = len(invalid_opdata)
    s1_valid    = s1_total - s1_invalid

    # [2] ingest_raw (bronze)
    valid_cands   = [r for r in candidates if r["file_id"] not in invalid_opdata]
    s2_url        = sum(1 for r in valid_cands if r["file_url"])
    s2_upload     = sum(1 for r in valid_cands if not r["file_url"])
    s2_url_ok     = s2_url - len(ingest_errs)
    s2_url_err    = len(ingest_errs)
    s2_total_bronze = s2_url_ok + s2_upload   # ficheiros que chegaram ao Bronze

    # [3] validate_bronze
    s3_total    = s2_total_bronze
    s3_invalid  = len(bronze_errs)
    s3_valid    = s3_total - s3_invalid

    # [4] transform (silver)
    s4_total    = s3_valid
    s4_err_log  = len(transform_errs)
    s4_err_miss = len(transform_missing)
    s4_err      = s4_err_log + s4_err_miss
    s4_ok       = s4_total - s4_err

    # [5] validate_silver
    s5_total    = s4_ok
    s5_invalid  = len(silver_errs)
    s5_valid    = s5_total - s5_invalid

    # [6] gold
    s6_errs     = len(load_errs_list)

    s2_ok = s2_url_ok + s2_upload

    w(DASH)
    w("RESUMO")
    w(DASH)
    w(f"[1] validate_opdata      Novos: {s1_total:<4}  Válidos: {s1_valid:<4}  Inválidos: {s1_invalid:<4}  → etl_logs: {s1_invalid}")
    w(f"[2] Ingestão Bronze      Processados: {s1_valid:<4}  OK: {s2_ok:<4}  Erro: {s2_url_err:<4}  → etl_logs: {s2_url_err}")
    w(f"[3] Validação Bronze     Validados: {s3_total:<4}  OK: {s3_valid:<4}  Inválidos: {s3_invalid:<4}  → etl_logs: {s3_invalid}")
    w(f"[4] Transformação Silver Processados: {s4_total:<4}  OK: {s4_ok:<4}  Erro: {s4_err:<4}  → etl_logs: {s4_err_log}" +
      (f"  (+ {s4_err_miss} sem registo)" if s4_err_miss else ""))
    w(f"[5] Validação Silver     Validados: {s5_total:<4}  OK: {s5_valid:<4}  Inválidos: {s5_invalid:<4}  → etl_logs: {s5_invalid}")
    w(f"[6] Warehouse (gold)     dim_report: {dim_report_run}  dim_location: {dim_location_total}  "
      f"dim_indicator: {dim_indicator_total}  fact_values: {total_facts}  Erros: {s6_errs}")
    w("")

    # ══════════════════════════════════════════════════════════════════════
    # PASSO 1 — validate_opdata
    # ══════════════════════════════════════════════════════════════════════
    w(SEP)
    w("1/6 — VALIDACAO op_data")
    w(SEP)
    if not candidates:
        w("  Sem ficheiros novos para validar.")
    else:
        for r in candidates:
            fid = r["file_id"]
            label = f"[{r['source_code']}] {r['report_name']}" if r["report_name"] else f"report_id={r['report_id']}"
            fn  = r["extract_function"] or "—"
            url = (r["file_url"] or "upload direto")
            if fid in invalid_opdata:
                w(f"  INVALIDO  file_id={fid}  {label}")
                w(f"            fn={fn}")
                w(f"            Erro: {invalid_opdata[fid]}")
            else:
                w(f"  OK        file_id={fid}  {label}  fn={fn}")
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
                no_bronze = str(fid) not in bronze_keys
                nota = "  [ATENCAO: nao encontrado no bucket Bronze]" if no_bronze else ""
                w(f"  OK        file_id={fid}  URL={url}{nota}")
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
        w("  Nenhum objecto Bronze para validar nesta execucao.")
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
        for r in to_transform:
            fid = r["file_id"]
            fn  = r["extract_function"] or "—"
            silver_key = f"{fid}.parquet"
            if fid in transform_errs:
                w(f"  ERRO      file_id={fid}  fn={fn}")
                w(f"            Erro: {transform_errs[fid]}")
            elif fid in transform_missing:
                w(f"  ERRO      file_id={fid}  fn={fn}  -> {silver_key}  [parquet nao criado no Silver]")
            else:
                w(f"  OK        file_id={fid}  fn={fn}  -> {silver_key}")
    w("")

    # ══════════════════════════════════════════════════════════════════════
    # PASSO 5 — validate_silver
    # ══════════════════════════════════════════════════════════════════════
    w(SEP)
    w("5/6 — VALIDACAO Silver (validate_silver)")
    w(SEP)
    to_validate_silver = [r for r in to_transform
                          if r["file_id"] not in transform_errs
                          and r["file_id"] not in transform_missing]
    if not to_validate_silver:
        w("  Nenhum ficheiro chegou a esta fase.")
    else:
        for r in to_validate_silver:
            fid = r["file_id"]
            if fid in silver_errs:
                w(f"  INVALIDO  file_id={fid}")
                w(f"            Erro: {silver_errs[fid]}")
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
            fname = lg["file_name"] or lg["file_id"] or "—"
            w(f"  ERRO      step={lg['step']}  ficheiro={fname}")
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
            fid   = str(lg["file_id"]  or "—")
            step  = str(lg["step"]     or "—")
            lt    = str(lg["log_time"])[:19]
            msg   = (lg["error_message"] or "")
            w(f"  {fid:<10} {step:<22} {lt:<22} {msg}")
    w("")

    w(SEP)
    w("FIM DO RELATORIO")
    w(SEP)

    # ── Fechar conexões ───────────────────────────────────────────────────
    cur_pipe.close(); conn_pipe.close()
    cur_op.close();   conn_op.close()
    cur_dw.close();   conn_dw.close()

    # ── Escrever ficheiro ─────────────────────────────────────────────────
    report_path = os.path.abspath(REPORT_PATH)
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Relatorio guardado em: {report_path}")
    return report_path
