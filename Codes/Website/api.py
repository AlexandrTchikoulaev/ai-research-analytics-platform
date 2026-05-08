from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from datetime import datetime, date
import psycopg2
import psycopg2.extras
import subprocess
import sys
import os
import re
import json
import boto3

# ── RAG imports ──────────────────────────────────────────
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.llms.ollama import Ollama
from langchain_community.vectorstores import PGVector
from langchain_community.embeddings.ollama import OllamaEmbeddings

# ── Configurações ─────────────────────────────────────────
_DB_BASE = {"host": "localhost", "port": 5433, "user": "projeto_utilizador", "password": "projeto"}

DB_WAREHOUSE   = {**_DB_BASE, "database": "warehouse_db"}
DB_OPERATIONAL = {**_DB_BASE, "database": "operational_db"}
DB_PIPELINE    = {**_DB_BASE, "database": "pipeline_db"}
DB_VECTOR      = {**_DB_BASE, "database": "vector_db"}

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

BUCKET_UNSTRUCTURED = "bronze-unstructured"
BUCKET_RAW = "bronze"

PROMPT_TEMPLATE = """
Answer the question based only on the following context:

{context}

---

Answer the question based on the above context: {question}
"""

_HERE = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DADOS_SCRIPT = os.path.normpath(os.path.join(_HERE, "..", "Pipeline", "pipeline_dados.py"))
PIPELINE_PDFS_SCRIPT  = os.path.normpath(os.path.join(_HERE, "..", "Pipeline Unstructured", "pipeline_pdfs.py"))


# ── Helpers ───────────────────────────────────────────────
def _connect(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
        user=cfg["user"], password=cfg["password"],
    )

def get_warehouse_connection():   return _connect(DB_WAREHOUSE)
def get_operational_connection(): return _connect(DB_OPERATIONAL)
def get_pipeline_connection():    return _connect(DB_PIPELINE)


def get_s3():
    return boto3.client("s3", **MINIO_CONFIG)


def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)


def strip_jsonc_comments(text: str) -> str:
    """Remove comentários // de uma string JSONC."""
    lines = text.splitlines()
    clean = []
    for line in lines:
        # Remove comentário // fora de strings (simplificado)
        stripped = re.sub(r'(?<!:)//.*$', '', line)
        clean.append(stripped)
    return "\n".join(clean)


def parse_date_flexible(s: str):
    """Tenta parsear data nos formatos DD/MM/AAAA, AAAA-MM-DD, DD-MM-AAAA."""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Formato de data não reconhecido: {s}")


# ── App ──────────────────────────────────────────────────
app = FastAPI(title="Repositório de Rankings e Relatórios")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Schemas ───────────────────────────────────────────────
class ReportIn(BaseModel):
    source_code: str
    file_name: str
    report_url: str = ""
    publication_date: date
    area_tematica: str = ""
    estado: str = ""
    palavras_chave: str = ""
    resumo: str = ""


class ChatIn(BaseModel):
    question: str


class OpDataIn(BaseModel):
    report_id: int
    file_name: str = ""
    file_url: str = ""
    extract_function: str = ""
    file_type: str = ""


# ── Helpers RAG ───────────────────────────────────────────
def get_embedding_function():
    return OllamaEmbeddings(model="mxbai-embed-large")


def query_rag(query_text: str):
    embedding_function = get_embedding_function()
    connection_string = (
        f"postgresql+psycopg2://{DB_VECTOR['user']}:{DB_VECTOR['password']}"
        f"@{DB_VECTOR['host']}:{DB_VECTOR['port']}/{DB_VECTOR['database']}"
    )
    db = PGVector(
        connection_string=connection_string,
        embedding_function=embedding_function,
        collection_name="documents",
    )
    results = db.similarity_search_with_score(query_text, k=5)
    if not results:
        return {"answer": "Não encontrei informação relevante nos documentos indexados.", "sources": []}

    context_text = "\n\n---\n\n".join([doc.page_content for doc, _score in results])
    prompt_template = ChatPromptTemplate.from_template(PROMPT_TEMPLATE)
    prompt = prompt_template.format(context=context_text, question=query_text)
    model = Ollama(model="qwen2.5:7b")
    response_text = model.invoke(prompt)
    sources = [doc.metadata.get("id") for doc, _score in results]
    return {"answer": response_text, "sources": sources}


# ════════════════════════════════════════════════════════════
# ENDPOINTS — op_report
# ════════════════════════════════════════════════════════════

@app.post("/op_report", status_code=201)
def add_report(report: ReportIn):
    """Insere um novo relatório (via JSON)."""
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO op_report (source_code, file_name, report_url, publication_date,
                                   area_tematica, estado, palavras_chave, resumo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING report_id;
        """, (report.source_code, report.file_name, report.report_url or None,
              report.publication_date, report.area_tematica, report.estado,
              report.palavras_chave, report.resumo))
        report_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return {"report_id": report_id, "message": "Relatório inserido com sucesso."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/op_report/upload", status_code=201)
async def upload_report(
    file: UploadFile = File(...),
    source_code: str = Form(...),
    publication_date: str = Form(...),
    file_name: str = Form(...),
    area_tematica: str = Form(""),
    estado: str = Form(""),
    palavras_chave: str = Form(""),
    resumo: str = Form(""),
):
    """Insere um relatório PDF via upload direto para MinIO."""
    content = await file.read()
    if not content[:4] == b"%PDF":
        raise HTTPException(status_code=400, detail="O ficheiro não é um PDF válido.")

    try:
        pub_date = parse_date_flexible(publication_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    s3 = get_s3()
    ensure_bucket(s3, BUCKET_UNSTRUCTURED)

    try:
        s3.put_object(
            Bucket=BUCKET_UNSTRUCTURED,
            Key=file_name,
            Body=content,
            ContentType="application/pdf",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao guardar PDF no MinIO: {e}")

    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO op_report (source_code, file_name, report_url, publication_date,
                                   area_tematica, estado, palavras_chave, resumo)
            VALUES (%s, %s, NULL, %s, %s, %s, %s, %s)
            RETURNING report_id;
        """, (source_code, file_name, pub_date, area_tematica, estado, palavras_chave, resumo))
        report_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return {"report_id": report_id, "message": "PDF carregado e relatório registado com sucesso."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/op_report/batch", status_code=201)
async def batch_reports(payload: list[dict]):
    """Insere múltiplos relatórios em lote com savepoints."""
    conn = get_operational_connection()
    cur = conn.cursor()
    inserted = 0
    errors = []

    for i, item in enumerate(payload):
        sp = f"sp_{i}"
        try:
            cur.execute(f"SAVEPOINT {sp}")
            source_code = item.get("source_code", "")
            file_name = item.get("file_name", "")
            report_url = item.get("report_url", "") or None
            pub_date_raw = item.get("publication_date", "")
            pub_date = parse_date_flexible(str(pub_date_raw)) if pub_date_raw else None

            if not source_code or not file_name or not pub_date:
                raise ValueError("source_code, file_name e publication_date são obrigatórios")

            cur.execute("""
                INSERT INTO op_report (source_code, file_name, report_url, publication_date,
                                       area_tematica, estado, palavras_chave, resumo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (source_code, file_name, report_url, pub_date,
                  item.get("area_tematica", ""), item.get("estado", ""),
                  item.get("palavras_chave", ""), item.get("resumo", "")))
            cur.execute(f"RELEASE SAVEPOINT {sp}")
            inserted += 1
        except Exception as e:
            cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            errors.append({"index": i, "file_name": item.get("file_name", "?"), "error": str(e)})

    conn.commit()
    cur.close()
    conn.close()
    return {"inserted": inserted, "errors": errors}


# ════════════════════════════════════════════════════════════
# ENDPOINTS — op_data
# ════════════════════════════════════════════════════════════

@app.post("/op_data", status_code=201)
def add_op_data(data: OpDataIn):
    """Insere um ficheiro de dados (URL)."""
    conn = get_operational_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM op_report WHERE report_id = %s", (data.report_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"report_id {data.report_id} não existe.")

        cur.execute("""
            INSERT INTO op_data (report_id, file_name, file_url, extract_function, file_type)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING file_id;
        """, (data.report_id, data.file_name or None, data.file_url or None,
              data.extract_function or None, data.file_type or None))
        file_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return {"file_id": file_id, "message": "Ficheiro inserido com sucesso."}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/op_data/upload", status_code=201)
async def upload_op_data(
    file: UploadFile = File(...),
    report_id: int = Form(...),
    extract_function: str = Form(""),
    file_type: str = Form(""),
):
    """Carrega um ficheiro de dados diretamente para Bronze (MinIO raw)."""
    content = await file.read()
    file_name = file.filename or "upload"

    conn = get_operational_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM op_report WHERE report_id = %s", (report_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"report_id {report_id} não existe.")

    # Criar registo primeiro para obter o file_id
    try:
        cur.execute("""
            INSERT INTO op_data (report_id, file_name, file_url, extract_function, file_type)
            VALUES (%s, %s, NULL, %s, %s)
            RETURNING file_id, created_at;
        """, (report_id, file_name, extract_function or None, file_type or None))
        file_id, created_at = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    cur.close()
    conn.close()

    # Guardar no MinIO com file_id como key
    s3 = get_s3()
    ensure_bucket(s3, BUCKET_RAW)
    created_str = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)
    try:
        s3.put_object(
            Bucket=BUCKET_RAW,
            Key=str(file_id),
            Body=content,
            Metadata={
                "file_id": str(file_id),
                "report_id": str(report_id),
                "extract_function": extract_function or "",
                "file_type": file_type or "",
                "file_name": file_name,
                "file_format": file_name.rsplit(".", 1)[-1].lower() if "." in file_name else "json",
                "created_at": created_str,
                "source_url": "",
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao guardar no MinIO: {e}")

    return {"file_id": file_id, "message": "Ficheiro carregado e registado com sucesso."}


@app.post("/op_data/pairs", status_code=201)
async def upload_op_data_pairs(
    file: UploadFile = File(...),
    report_id: int = Form(...),
    extract_functions: str = Form(...),
    file_type: str = Form(""),
):
    """Carrega um ficheiro e associa-o a múltiplas funções de extração (modo pares)."""
    content = await file.read()
    file_name = file.filename or "upload"
    functions = [f.strip() for f in extract_functions.split(",") if f.strip()]

    if not functions:
        raise HTTPException(status_code=400, detail="Pelo menos uma extract_function é necessária.")

    conn = get_operational_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM op_report WHERE report_id = %s", (report_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"report_id {report_id} não existe.")

    s3 = get_s3()
    ensure_bucket(s3, BUCKET_RAW)

    created_ids = []
    try:
        for fn in functions:
            cur.execute("""
                INSERT INTO op_data (report_id, file_name, file_url, extract_function, file_type)
                VALUES (%s, %s, NULL, %s, %s)
                RETURNING file_id, created_at;
            """, (report_id, file_name, fn, file_type or None))
            file_id, created_at = cur.fetchone()
            created_str = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)

            s3.put_object(
                Bucket=BUCKET_RAW,
                Key=str(file_id),
                Body=content,
                Metadata={
                    "file_id": str(file_id),
                    "report_id": str(report_id),
                    "extract_function": fn,
                    "file_type": file_type or "",
                    "file_name": file_name,
                    "file_format": file_name.rsplit(".", 1)[-1].lower() if "." in file_name else "json",
                    "created_at": created_str,
                    "source_url": "",
                },
            )
            created_ids.append(file_id)

        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    cur.close()
    conn.close()
    return {"file_ids": created_ids, "message": f"{len(created_ids)} registos criados (modo pares)."}


@app.post("/op_data/batch", status_code=201)
async def batch_op_data(payload: list[dict]):
    """Insere múltiplos ficheiros de dados em lote (suporta JSONC via pré-processamento)."""
    conn = get_operational_connection()
    cur = conn.cursor()
    inserted = 0
    errors = []

    for i, item in enumerate(payload):
        sp = f"sp_{i}"
        try:
            cur.execute(f"SAVEPOINT {sp}")
            report_id = item.get("report_id")
            file_url = item.get("file_url") or None
            file_name = item.get("file_name") or None
            extract_function = item.get("extract_function") or None
            file_type = item.get("file_type") or None

            if not report_id:
                raise ValueError("report_id é obrigatório")
            if not file_url and not file_name:
                raise ValueError("file_url ou file_name são obrigatórios")

            cur.execute("SELECT 1 FROM op_report WHERE report_id = %s", (report_id,))
            if not cur.fetchone():
                raise ValueError(f"report_id {report_id} não existe")

            cur.execute("""
                INSERT INTO op_data (report_id, file_name, file_url, extract_function, file_type)
                VALUES (%s, %s, %s, %s, %s)
            """, (report_id, file_name, file_url, extract_function, file_type))
            cur.execute(f"RELEASE SAVEPOINT {sp}")
            inserted += 1
        except Exception as e:
            cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            errors.append({"index": i, "error": str(e)})

    conn.commit()
    cur.close()
    conn.close()
    return {"inserted": inserted, "errors": errors}


# ════════════════════════════════════════════════════════════
# ENDPOINT — Verificação de Duplicados
# ════════════════════════════════════════════════════════════

@app.get("/check_duplicate")
def check_duplicate(field: str, value: str):
    """Verifica se um valor já existe em op_report (file_name ou report_url)."""
    allowed = {"file_name", "report_url"}
    if field not in allowed:
        raise HTTPException(status_code=400, detail=f"Campo inválido. Permitidos: {allowed}")
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT report_id FROM op_report WHERE {field} = %s LIMIT 1", (value,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return {"exists": True, "report_id": row[0]}
        return {"exists": False}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ════════════════════════════════════════════════════════════
# ENDPOINTS — Leitura
# ════════════════════════════════════════════════════════════

@app.get("/op_report")
def get_reports():
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT report_id, source_code, file_name, report_url, publication_date,
                   area_tematica, estado, palavras_chave, resumo
            FROM op_report ORDER BY report_id DESC;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/op_data")
def get_op_data():
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT d.file_id, d.report_id, d.file_name, d.file_url,
                   d.extract_function, d.file_type, r.source_code
            FROM op_data d
            LEFT JOIN op_report r ON r.report_id = d.report_id
            ORDER BY d.file_id DESC;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sources")
def get_sources():
    try:
        conn = get_warehouse_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT DISTINCT source_code, source_name FROM dim_report WHERE source_code IS NOT NULL ORDER BY source_name;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/indicators")
def get_indicators(source_code: str = None):
    try:
        conn_dw = get_warehouse_connection()
        conn_op = get_operational_connection()
        cur_dw = conn_dw.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur_op = conn_op.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if source_code:
            cur_op.execute("SELECT report_id FROM op_report WHERE source_code = %s", (source_code,))
            report_ids = [r["report_id"] for r in cur_op.fetchall()]
            if not report_ids:
                rows = []
            else:
                cur_dw.execute("""
                    SELECT DISTINCT i.indicator_code, i.indicator_name
                    FROM dim_indicator i
                    JOIN fact_values f ON f.indicator_code = i.indicator_code
                    WHERE f.report_id = ANY(%s)
                    ORDER BY i.indicator_name;
                """, (report_ids,))
                rows = [dict(r) | {"source_code": source_code} for r in cur_dw.fetchall()]
        else:
            cur_dw.execute("""
                SELECT DISTINCT ON (i.indicator_code) i.indicator_code, i.indicator_name, f.report_id
                FROM dim_indicator i
                JOIN fact_values f ON f.indicator_code = i.indicator_code
                ORDER BY i.indicator_code;
            """)
            dw_rows = cur_dw.fetchall()
            if dw_rows:
                rids = list({r["report_id"] for r in dw_rows})
                cur_op.execute("SELECT report_id, source_code FROM op_report WHERE report_id = ANY(%s)", (rids,))
                src_map = {r["report_id"]: r["source_code"] for r in cur_op.fetchall()}
                rows = [{"indicator_code": r["indicator_code"], "indicator_name": r["indicator_name"], "source_code": src_map.get(r["report_id"])} for r in dw_rows]
            else:
                rows = []

        cur_dw.close(); conn_dw.close()
        cur_op.close(); conn_op.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fact_values")
def get_fact_values(indicator_code: str):
    try:
        conn = get_warehouse_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT c.location_code, c.location_name, d.year, f.value
            FROM fact_values f
            JOIN dim_location c ON f.location_code = c.location_code
            JOIN dim_indicator i ON f.indicator_code = i.indicator_code
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE i.indicator_code = %s
            ORDER BY d.year ASC, c.location_name ASC;
        """, (indicator_code,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ════════════════════════════════════════════════════════════
# ENDPOINTS — Dashboard
# ════════════════════════════════════════════════════════════

@app.get("/dashboard")
def get_dashboard(indicator_name: str, year: int):
    try:
        conn = get_warehouse_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT location_name, value FROM view
               WHERE REPLACE(REPLACE(indicator_name, E'\n', ' '), E'\r', '') = %s
               AND year = %s ORDER BY location_name;""",
            (indicator_name.strip(), year),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dashboard/filters")
def get_dashboard_filters(indicator_name: str = None):
    try:
        conn = get_warehouse_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT DISTINCT REPLACE(REPLACE(indicator_name, E'\n', ' '), E'\r', '') AS indicator_name
            FROM view ORDER BY 1;
        """)
        indicators = [r["indicator_name"].strip() for r in cur.fetchall()]

        if indicator_name:
            cur.execute("""
                SELECT DISTINCT year FROM view
                WHERE REPLACE(REPLACE(indicator_name, E'\n', ' '), E'\r', '') = %s
                ORDER BY year DESC;
            """, (indicator_name.strip(),))
        else:
            cur.execute("SELECT DISTINCT year FROM view ORDER BY year DESC;")

        years = [r["year"] for r in cur.fetchall()]
        cur.close()
        conn.close()
        return {"indicators": indicators, "years": years}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dashboard/timeseries")
def get_dashboard_timeseries(indicator_name: str, countries: str):
    try:
        country_list = [c.strip() for c in countries.split(",") if c.strip()]
        if not country_list:
            raise HTTPException(status_code=400, detail="Pelo menos um país deve ser fornecido.")
        conn = get_warehouse_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT location_name, year, value FROM view
               WHERE REPLACE(REPLACE(indicator_name, E'\n', ' '), E'\r', '') = %s
                 AND location_name = ANY(%s)
               ORDER BY location_name, year ASC;""",
            (indicator_name.strip(), country_list),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ════════════════════════════════════════════════════════════
# ENDPOINTS — ETL
# ════════════════════════════════════════════════════════════

def _stream_script(script_path: str):
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail=f"Script não encontrado: {script_path}")

    script_name = os.path.basename(script_path)
    script_dir  = os.path.dirname(script_path)

    def stream_output():
        yield f"A iniciar {script_name}...\n"
        try:
            process = subprocess.Popen(
                [sys.executable, script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=script_dir,
            )
            for line in process.stdout:
                yield line
            process.wait()
            if process.returncode == 0:
                yield f"\n✓ {script_name} concluído com sucesso.\n"
            else:
                yield f"\n✗ {script_name} terminou com erro (código {process.returncode}).\n"
        except Exception as e:
            yield f"\n✗ Erro: {e}\n"

    return StreamingResponse(stream_output(), media_type="text/plain")


@app.post("/etl/run")
def etl_run():
    return _stream_script(PIPELINE_DADOS_SCRIPT)


@app.post("/etl/run/dados")
def etl_run_dados():
    return _stream_script(PIPELINE_DADOS_SCRIPT)


@app.post("/etl/run/pdfs")
def etl_run_pdfs():
    return _stream_script(PIPELINE_PDFS_SCRIPT)


@app.get("/etl_logs")
def get_etl_logs():
    try:
        conn = get_pipeline_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT *, 'dados' AS pipeline FROM etl_logs_dados
            UNION ALL
            SELECT *, 'pdfs'  AS pipeline FROM etl_logs_pdfs
            ORDER BY log_time DESC LIMIT 500
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ════════════════════════════════════════════════════════════
# ENDPOINT — Chat
# ════════════════════════════════════════════════════════════

@app.post("/chat")
def chat(body: ChatIn):
    if not body.question.strip():
        raise HTTPException(status_code=400, detail="A pergunta não pode estar vazia.")
    try:
        result = query_rag(body.question)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
