"""
Gerador de relatório detalhado da última execução da pipeline de PDFs.
Chamado pelo pipeline_pdfs.py no final de cada execução.
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
DB_VECTOR = {
    "host": "localhost", "port": 5433, "dbname": "vector_db",
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
    "pipeline_reports_report.txt"
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
    conn_pipe = conn_op = conn_vec = None
    conn_pipe = psycopg2.connect(**DB_PIPELINE)
    cur_pipe  = conn_pipe.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    conn_op   = psycopg2.connect(**DB_OPERATIONAL)
    cur_op    = conn_op.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    conn_vec  = psycopg2.connect(**DB_VECTOR)
    cur_vec   = conn_vec.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        s3 = boto3.client("s3", **MINIO_CONFIG)
    except Exception:
        s3 = None

    # ── Candidatos (op_report desta run) ──────────────────────────────────
    if prev_last_run:
        cur_op.execute("""
            SELECT report_id, file_name, report_url, source_code, created_at
            FROM op_report
            WHERE created_at > %s
            ORDER BY report_id
        """, (prev_last_run,))
    else:
        cur_op.execute("""
            SELECT report_id, file_name, report_url, source_code, created_at
            FROM op_report
            ORDER BY report_id
        """)
    candidates = cur_op.fetchall()

    # ── Logs desta execução ────────────────────────────────────────────────
    if prev_last_run is not None:
        cur_pipe.execute("""
            SELECT report_id, file_name, step, error_message, log_time
            FROM etl_logs_pdfs
            WHERE log_time > %s
            ORDER BY log_time ASC
        """, (prev_last_run,))
    else:
        cur_pipe.execute("""
            SELECT report_id, file_name, step, error_message, log_time
            FROM etl_logs_pdfs
            ORDER BY log_time ASC
        """)
    run_logs = cur_pipe.fetchall()

    by_step = {}
    for lg in run_logs:
        by_step.setdefault(lg["step"], []).append(lg)

    def errs_by_fname(step):
        # Use a set for filtering (handles duplicate file_names) and a dict for error messages
        errs = {}
        for lg in by_step.get(step, []):
            errs.setdefault(lg["file_name"], lg["error_message"])
        return errs

    def err_fnames(step):
        return {lg["file_name"] for lg in by_step.get(step, [])}

    invalid_op_report     = errs_by_fname("validate_op_report")
    invalid_op_report_set = err_fnames("validate_op_report")
    bronze_errs           = errs_by_fname("bronze")
    bronze_errs_set       = err_fnames("bronze")
    bronze_val_errs       = errs_by_fname("validate_bronze_unstructured")
    bronze_val_errs_set   = err_fnames("validate_bronze_unstructured")

    # ── MinIO ─────────────────────────────────────────────────────────────
    bronze_keys = _minio_keys(s3, "bronze-unstructured") if s3 else set()

    # ── Vector DB stats ────────────────────────────────────────────────────
    try:
        cur_vec.execute("SELECT COUNT(*) AS cnt FROM langchain_pg_embedding")
        total_chunks = cur_vec.fetchone()["cnt"]
    except Exception:
        total_chunks = "—"

    try:
        cur_vec.execute("""
            SELECT COUNT(DISTINCT cmetadata->>'source') AS cnt
            FROM langchain_pg_embedding
            WHERE cmetadata->>'source' IS NOT NULL
        """)
        total_docs = cur_vec.fetchone()["cnt"]
    except Exception:
        total_docs = "—"

    cand_fnames = [r["file_name"] for r in candidates if r["file_name"]]
    chunk_counts = {}
    if cand_fnames:
        try:
            cur_vec.execute("""
                SELECT cmetadata->>'source' AS source, COUNT(*) AS cnt
                FROM langchain_pg_embedding
                WHERE cmetadata->>'source' = ANY(%s)
                GROUP BY cmetadata->>'source'
            """, (cand_fnames,))
            for row in cur_vec.fetchall():
                chunk_counts[row["source"]] = row["cnt"]
        except Exception:
            pass

    # ── Contagens derivadas para o RESUMO ─────────────────────────────────
    s1_total   = len(candidates)
    s1_invalid = len(invalid_op_report_set)
    s1_valid   = s1_total - s1_invalid

    valid_cands     = [r for r in candidates    if (r["file_name"] or f"report_id={r['report_id']}") not in invalid_op_report_set]
    s2_total        = len(valid_cands)
    s2_err          = len(bronze_errs_set)
    s2_ok           = s2_total - s2_err

    bronze_ok_cands = [r for r in valid_cands   if (r["file_name"] or "") not in bronze_errs_set]
    s3_total        = len(bronze_ok_cands)
    s3_invalid      = len(bronze_val_errs_set)
    s3_valid        = s3_total - s3_invalid

    silver_cands    = [r for r in bronze_ok_cands if (r["file_name"] or "") not in bronze_val_errs_set]
    s4_total        = len(silver_cands)
    s4_indexed      = sum(1 for r in silver_cands if r["file_name"] in chunk_counts)
    s4_missing      = s4_total - s4_indexed

    # ══════════════════════════════════════════════════════════════════════
    # CABEÇALHO
    # ══════════════════════════════════════════════════════════════════════
    w(SEP)
    w("RELATÓRIO DA PIPELINE DE PDFs")
    w(f"Gerado em          : {now_str}")
    w(f"Início da execução : {start_str}")
    w(f"Execução anterior  : {prev_str}")
    w(f"Estado             : {'CONCLUÍDA COM SUCESSO' if success else 'FALHOU'}")
    w(SEP)
    w("")

    w(DASH)
    w("RESUMO")
    w(DASH)
    w(f"[1] validate_op_report       Novos: {s1_total:<4}  Válidos: {s1_valid:<4}  Inválidos: {s1_invalid:<4}  → etl_logs: {s1_invalid}")
    w(f"[2] Ingestão Bronze          Processados: {s2_total:<4}  OK: {s2_ok:<4}  Erro: {s2_err:<4}  → etl_logs: {s2_err}")
    w(f"[3] Validação Bronze         Validados: {s3_total:<4}  OK: {s3_valid:<4}  Inválidos: {s3_invalid:<4}  → etl_logs: {s3_invalid}")
    w(f"[4] Indexação Silver         PDFs desta run: {s4_total:<4}  Indexados: {s4_indexed:<4}  Não indexados: {s4_missing:<4}")
    w(f"                             Total chunks no vector DB: {total_chunks}  Total documentos: {total_docs}")
    w("")

    # ══════════════════════════════════════════════════════════════════════
    # PASSO 1 — validate_op_report
    # ══════════════════════════════════════════════════════════════════════
    w(SEP)
    w("1/4 — VALIDACAO op_report")
    w(SEP)
    if not candidates:
        w("  Sem relatórios novos para validar.")
    else:
        for r in candidates:
            fname   = r["file_name"] or f"report_id={r['report_id']}"
            src     = r["source_code"] or "—"
            log_key = r["file_name"] or f"report_id={r['report_id']}"
            if log_key in invalid_op_report_set:
                w(f"  INVALIDO  report_id={r['report_id']}  [{src}]  {fname}")
                w(f"            Erro: {invalid_op_report.get(log_key, '—')}")
            else:
                w(f"  OK        report_id={r['report_id']}  [{src}]  {fname}")
    w("")

    # ══════════════════════════════════════════════════════════════════════
    # PASSO 2 — bronze
    # ══════════════════════════════════════════════════════════════════════
    w(SEP)
    w("2/4 — INGESTAO Bronze (bronze-unstructured)")
    w(SEP)
    if not candidates:
        w("  Sem PDFs para ingerir.")
    else:
        for r in candidates:
            fname   = r["file_name"] or f"report_id={r['report_id']}"
            log_key = r["file_name"] or f"report_id={r['report_id']}"
            if log_key in invalid_op_report_set:
                w(f"  IGNORADO  report_id={r['report_id']}  {fname}  [invalidado em validate_op_report]")
            elif fname in bronze_errs_set:
                w(f"  ERRO      report_id={r['report_id']}  {fname}")
                w(f"            Erro: {bronze_errs[fname]}")
            else:
                nota = "  [ATENCAO: nao encontrado no bucket]" if fname not in bronze_keys else ""
                w(f"  OK        report_id={r['report_id']}  {fname}{nota}")
    w("")

    # ══════════════════════════════════════════════════════════════════════
    # PASSO 3 — validate_bronze_unstructured
    # ══════════════════════════════════════════════════════════════════════
    w(SEP)
    w("3/4 — VALIDACAO Bronze (validate_bronze_unstructured)")
    w(SEP)
    if not bronze_ok_cands:
        w("  Nenhum PDF Bronze para validar nesta execução.")
    else:
        for r in bronze_ok_cands:
            fname = r["file_name"] or f"report_id={r['report_id']}"
            if fname in bronze_val_errs_set:
                w(f"  INVALIDO  report_id={r['report_id']}  {fname}")
                w(f"            Erro: {bronze_val_errs.get(fname, '—')}")
            else:
                w(f"  OK        report_id={r['report_id']}  {fname}")
    w("")

    # ══════════════════════════════════════════════════════════════════════
    # PASSO 4 — silver (vector DB)
    # ══════════════════════════════════════════════════════════════════════
    w(SEP)
    w("4/4 — INDEXACAO Silver (vector DB)")
    w(SEP)
    if not silver_cands:
        w("  Nenhum PDF chegou a esta fase.")
    else:
        for r in silver_cands:
            fname  = r["file_name"] or f"report_id={r['report_id']}"
            chunks = chunk_counts.get(fname)
            if chunks is not None:
                w(f"  INDEXADO  report_id={r['report_id']}  {fname}  [{chunks} chunks]")
            else:
                w(f"  AVISO     report_id={r['report_id']}  {fname}  [não encontrado no vector DB]")
    w("")
    w("  ESTADO DO VECTOR DB (após esta run):")
    w(f"  {'langchain_pg_embedding':<28} total chunks     : {total_chunks}")
    w(f"  {'documentos distintos':<28} total documentos : {total_docs}")
    w("")

    # ══════════════════════════════════════════════════════════════════════
    # TODOS OS ERROS ETL_LOGS_PDFS DESTA EXECUÇÃO
    # ══════════════════════════════════════════════════════════════════════
    w(SEP)
    w("REGISTOS etl_logs_pdfs DESTA EXECUCAO")
    w(SEP)
    if not run_logs:
        w("  Nenhum erro registado.")
    else:
        header = f"  {'report_id':<12} {'file_name':<30} {'step':<30} {'log_time':<22} {'erro'}"
        w(header)
        w("  " + "-" * 70)
        for lg in run_logs:
            rid   = str(lg["report_id"] or "—")
            fname = str(lg["file_name"] or "—")
            step  = str(lg["step"]      or "—")
            lt    = str(lg["log_time"])[:19]
            msg   = (lg["error_message"] or "")
            w(f"  {rid:<12} {fname:<30} {step:<30} {lt:<22} {msg}")
    w("")

    w(SEP)
    w("FIM DO RELATORIO")
    w(SEP)

    # ── Fechar conexões ───────────────────────────────────────────────────
    if conn_pipe: conn_pipe.close()
    if conn_op:   conn_op.close()
    if conn_vec:  conn_vec.close()

    # ── Escrever ficheiro ─────────────────────────────────────────────────
    report_path = os.path.abspath(REPORT_PATH)
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Relatório guardado em: {report_path}")
    return report_path
