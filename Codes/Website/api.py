from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date
import psycopg2
import psycopg2.extras
import subprocess
import sys
import os
import re
import json
import boto3

# Importar mapeamentos do silver_functions
sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "Pipeline")))
from silver_functions import EXTRACT_FUNCTIONS as _EXTRACT_FUNCTIONS, FUNCTION_FILE_TYPE
from silver_function_generator import generate_and_validate as _generate_and_validate

from sql_chat import chatbot_sql

# ── RAG imports ──────────────────────────────────────────
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.llms.ollama import Ollama
from langchain_community.vectorstores import PGVector
from langchain_community.embeddings.ollama import OllamaEmbeddings

# ── Configurações ─────────────────────────────────────────
_DB_BASE = {"host": "localhost", "port": 5433, "user": "projeto_utilizador", "password": "projeto"}

DB_WAREHOUSE   = {**_DB_BASE, "database": "warehouse_db"}
DB_OPERATIONAL = {**_DB_BASE, "database": "gestao_db"}
DB_PIPELINE    = {**_DB_BASE, "database": "gestao_db"}
DB_VECTOR      = {**_DB_BASE, "database": "vector_db"}

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

BUCKET_UNSTRUCTURED = "bronze-unstructured"
BUCKET_RAW = "bronze"


PROMPT_TEMPLATE = """
You are a helpful assistant. Answer the question based only on the following context.
Always respond in European Portuguese, regardless of the language of the context.
If the context does not contain enough information to answer, say so in Portuguese.

Context:
{context}

---

Question: {question}
Answer (in Portuguese):"""

_HERE = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DADOS_SCRIPT = os.path.normpath(os.path.join(_HERE, "..", "Pipeline", "pipeline_data.py"))
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
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
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


class GenerateFunctionUrlIn(BaseModel):
    url: str
    file_type: str


class OpDataIn(BaseModel):
    report_id: int
    file_url: str = ""
    extract_function: str = ""


class OpDataPatch(BaseModel):
    report_id: Optional[int] = None
    file_url: Optional[str] = None
    extract_function: Optional[str] = None
    file_type: Optional[str] = None


class ReportPatch(BaseModel):
    report_url: Optional[str] = None
    file_name: Optional[str] = None
    source_code: Optional[str] = None
    publication_date: Optional[date] = None
    area_tematica: Optional[str] = None
    estado: Optional[str] = None
    palavras_chave: Optional[str] = None
    resumo: Optional[str] = None


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
    if content[:4] != b"%PDF":
        raise HTTPException(status_code=400, detail="O ficheiro não é um PDF válido.")

    try:
        pub_date = parse_date_flexible(publication_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Inserir na DB primeiro para obter report_id, depois usar na metadata do MinIO
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao registar relatório: {e}")

    s3 = get_s3()
    ensure_bucket(s3, BUCKET_UNSTRUCTURED)

    try:
        s3.put_object(
            Bucket=BUCKET_UNSTRUCTURED,
            Key=file_name,
            Body=content,
            ContentType="application/pdf",
            Metadata={"report_id": str(report_id), "file_name": file_name},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao guardar PDF no MinIO: {e}")

    return {"report_id": report_id, "message": "PDF carregado e relatório registado com sucesso."}


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
                  item.get("areaTematica", ""), item.get("estado", ""),
                  item.get("palavras-chave", ""), item.get("resumo", "")))
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

        file_type = FUNCTION_FILE_TYPE.get(data.extract_function) if data.extract_function else None
        cur.execute("""
            INSERT INTO op_data (report_id, file_url, extract_function, file_type)
            VALUES (%s, %s, %s, %s)
            RETURNING file_id;
        """, (data.report_id, data.file_url or None,
              data.extract_function or None, file_type))
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
):
    """Carrega um ficheiro de dados diretamente para Bronze (MinIO raw)."""
    content = await file.read()
    fmt = (file.filename or "").rsplit(".", 1)[-1].lower() if "." in (file.filename or "") else "json"

    conn = get_operational_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM op_report WHERE report_id = %s", (report_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"report_id {report_id} não existe.")

    # Criar registo primeiro para obter o file_id
    file_type = FUNCTION_FILE_TYPE.get(extract_function) if extract_function else None
    original_name = file.filename or ""
    try:
        cur.execute("""
            INSERT INTO op_data (report_id, file_url, file_name, extract_function, file_type)
            VALUES (%s, NULL, %s, %s, %s)
            RETURNING file_id, created_at;
        """, (report_id, original_name, extract_function or None, file_type))
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
                "report_id": str(report_id),
                "extract_function": extract_function or "",
                "file_type": file_type or "",
                "created_at": created_str,
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao guardar no MinIO: {e}")

    return {"file_id": file_id, "file_type": file_type, "message": "Ficheiro carregado e registado com sucesso."}


@app.post("/op_data/pairs", status_code=201)
async def upload_op_data_pairs(
    files: list[UploadFile] = File(...),
    report_id: int = Form(...),
    extract_functions: str = Form(...),
):
    """Carrega múltiplos ficheiros e associa-os a múltiplas funções (produto cartesiano)."""
    functions = [f.strip() for f in extract_functions.split(",") if f.strip()]
    if not functions:
        raise HTTPException(status_code=400, detail="Pelo menos uma extract_function é necessária.")

    file_data = []
    for f in files:
        content = await f.read()
        fmt = (f.filename or "").rsplit(".", 1)[-1].lower() if "." in (f.filename or "") else "json"
        file_data.append((content, fmt, f.filename or ""))

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
        for content, fmt, file_name in file_data:
            for fn in functions:
                fn_file_type = FUNCTION_FILE_TYPE.get(fn)
                cur.execute("""
                    INSERT INTO op_data (report_id, file_url, file_name, extract_function, file_type)
                    VALUES (%s, NULL, %s, %s, %s)
                    RETURNING file_id, created_at;
                """, (report_id, file_name, fn, fn_file_type))
                file_id, created_at = cur.fetchone()
                created_str = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)
                s3.put_object(
                    Bucket=BUCKET_RAW,
                    Key=str(file_id),
                    Body=content,
                    Metadata={
                        "report_id": str(report_id),
                        "extract_function": fn,
                        "file_type": fn_file_type or "",
                        "created_at": created_str,
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
    n_files, n_fns = len(file_data), len(functions)
    return {"file_ids": created_ids, "message": f"{len(created_ids)} registos criados ({n_files} ficheiro(s) × {n_fns} função(ões))."}


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
            extract_function = item.get("extract_function") or None
            file_type = FUNCTION_FILE_TYPE.get(extract_function) if extract_function else None

            if not report_id:
                raise ValueError("report_id é obrigatório")

            cur.execute("SELECT 1 FROM op_report WHERE report_id = %s", (report_id,))
            if not cur.fetchone():
                raise ValueError(f"report_id {report_id} não existe")

            cur.execute("""
                INSERT INTO op_data (report_id, file_url, extract_function, file_type)
                VALUES (%s, %s, %s, %s)
            """, (report_id, file_url, extract_function, file_type))
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
# ENDPOINTS — Geração automática de funções de transformação
# ════════════════════════════════════════════════════════════

_AUTO_STORE = os.path.normpath(
    os.path.join(_HERE, "..", "Pipeline", "silver_functions_auto.json")
)


def _load_auto_store() -> dict:
    if not os.path.exists(_AUTO_STORE):
        return {}
    try:
        with open(_AUTO_STORE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_to_auto_store(name: str, code: str, file_type: str):
    from datetime import datetime
    store = _load_auto_store()
    store[name] = {
        "code":       code,
        "file_type":  file_type,
        "created_at": datetime.utcnow().isoformat(),
    }
    with open(_AUTO_STORE, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2)
    # Refresh in-memory EXTRACT_FUNCTIONS so api.py sees it immediately
    _EXTRACT_FUNCTIONS.update({name: None})
    FUNCTION_FILE_TYPE[name] = file_type


@app.post("/generate_function")
async def generate_function(
    file: UploadFile = File(...),
    file_type: str = Form(...),
):
    """Gera automaticamente uma função de transformação silver via Ollama."""
    if file_type not in ("indicator", "value"):
        raise HTTPException(status_code=400, detail="file_type deve ser 'indicator' ou 'value'.")

    content = await file.read()
    result = _generate_and_validate(content, file_type)

    if result["generated"] and result["valid"]:
        try:
            _save_to_auto_store(result["function_name"], result["code"], file_type)
        except Exception as e:
            result["error"] = f"Função válida mas erro ao guardar: {e}"

    return {
        "function_name": result["function_name"],
        "code":          result["code"],
        "fmt":           result["fmt"],
        "generated":     result["generated"],
        "valid":         result["valid"],
        "error":         result["error"],
        "preview":       result["preview"],
    }


@app.post("/generate_function_url")
async def generate_function_url(body: GenerateFunctionUrlIn):
    """Gera automaticamente uma função de transformação a partir de um URL."""
    if body.file_type not in ("indicator", "value"):
        raise HTTPException(status_code=400, detail="file_type deve ser 'indicator' ou 'value'.")
    if not body.url.strip():
        raise HTTPException(status_code=400, detail="URL é obrigatório.")

    import requests as _req
    try:
        resp = _req.get(body.url.strip(), timeout=30, stream=True)
        resp.raise_for_status()
        content = b""
        for chunk in resp.iter_content(chunk_size=8192):
            content += chunk
            if len(content) >= 512 * 1024:  # limite 500 KB para a amostra
                break
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao descarregar URL: {e}")

    result = _generate_and_validate(content, body.file_type)

    if result["generated"] and result["valid"]:
        try:
            _save_to_auto_store(result["function_name"], result["code"], body.file_type)
        except Exception as e:
            result["error"] = f"Função válida mas erro ao guardar: {e}"

    return {
        "function_name": result["function_name"],
        "code":          result["code"],
        "fmt":           result["fmt"],
        "generated":     result["generated"],
        "valid":         result["valid"],
        "error":         result["error"],
        "preview":       result["preview"],
    }


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
                   area_tematica, estado, palavras_chave, resumo, created_at
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
            SELECT d.file_id, d.report_id, d.file_url,
                   d.extract_function, d.file_type, r.source_code, d.created_at
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


@app.get("/op_data/{file_id}")
def get_op_data_by_id(file_id: int):
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT file_id, file_name, report_id, file_url, extract_function, file_type FROM op_data WHERE file_id = %s",
            (file_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail=f"file_id {file_id} não encontrado.")
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/op_data/{file_id}")
def patch_op_data(file_id: int, data: OpDataPatch):
    """Edita os campos de um ficheiro em op_data e repõe created_at para ser reprocessado na próxima pipeline."""
    try:
        conn_op = get_operational_connection()
        cur_op = conn_op.cursor()

        # Se extract_function for fornecida, derivar file_type automaticamente
        file_type = data.file_type
        if data.extract_function:
            computed = FUNCTION_FILE_TYPE.get(data.extract_function)
            if computed:
                file_type = computed

        cur_op.execute("""
            UPDATE op_data
            SET report_id        = COALESCE(%s, report_id),
                file_url         = %s,
                extract_function = %s,
                file_type        = %s,
                created_at       = CURRENT_TIMESTAMP
            WHERE file_id = %s
            RETURNING report_id, file_url, extract_function, file_type, created_at
        """, (data.report_id, data.file_url or None, data.extract_function or None, file_type or None, file_id))

        row = cur_op.fetchone()
        if not row:
            conn_op.rollback()
            cur_op.close(); conn_op.close()
            raise HTTPException(status_code=404, detail=f"file_id {file_id} não encontrado.")

        new_report_id, new_file_url, new_extract_function, new_file_type, new_created_at = row
        conn_op.commit()
        cur_op.close()
        conn_op.close()

        # Remover logs de erro deste file_id para que o bronze não o coloque na blacklist
        try:
            conn_pipe = get_pipeline_connection()
            cur_pipe = conn_pipe.cursor()
            cur_pipe.execute(
                "DELETE FROM etl_logs_dados WHERE file_id = %s AND status = 'error'",
                (str(file_id),)
            )
            conn_pipe.commit()
            cur_pipe.close()
            conn_pipe.close()
        except Exception:
            pass

        # Atualizar metadados no MinIO se o objeto existir
        try:
            s3 = get_s3()
            head = s3.head_object(Bucket=BUCKET_RAW, Key=str(file_id))
            current_meta = head.get("Metadata", {})
            created_str = new_created_at.isoformat() if hasattr(new_created_at, "isoformat") else str(new_created_at)
            new_meta = {
                "report_id":        str(new_report_id) if new_report_id is not None else "",
                "extract_function": new_extract_function or "",
                "file_type":        new_file_type or "",
                "created_at":       created_str,
            }
            s3.copy_object(
                Bucket=BUCKET_RAW,
                Key=str(file_id),
                CopySource={"Bucket": BUCKET_RAW, "Key": str(file_id)},
                Metadata=new_meta,
                MetadataDirective="REPLACE",
            )
        except Exception:
            pass

        return {"message": "Ficheiro atualizado. Será reinserido na próxima execução da pipeline."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/op_report/{report_id}")
def get_op_report_by_id(report_id: int):
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT report_id, source_code, file_name, report_url, publication_date, area_tematica, estado, palavras_chave, resumo FROM op_report WHERE report_id = %s",
            (report_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail=f"report_id {report_id} não encontrado.")
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/op_report/{report_id}")
def patch_op_report(report_id: int, data: ReportPatch):
    """Edita campos de op_report, repõe created_at e limpa erros para reprocessamento."""
    try:
        conn_op = get_operational_connection()
        cur_op = conn_op.cursor()
        cur_op.execute("""
            UPDATE op_report
            SET report_url       = COALESCE(%s, report_url),
                file_name        = COALESCE(%s, file_name),
                source_code      = COALESCE(%s, source_code),
                publication_date = COALESCE(%s, publication_date),
                area_tematica    = COALESCE(%s, area_tematica),
                estado           = COALESCE(%s, estado),
                palavras_chave   = COALESCE(%s, palavras_chave),
                resumo           = COALESCE(%s, resumo),
                created_at       = CURRENT_TIMESTAMP
            WHERE report_id = %s
            RETURNING file_name
        """, (
            data.report_url, data.file_name, data.source_code,
            data.publication_date, data.area_tematica, data.estado,
            data.palavras_chave, data.resumo,
            report_id,
        ))
        row = cur_op.fetchone()
        if not row:
            conn_op.rollback()
            cur_op.close(); conn_op.close()
            raise HTTPException(status_code=404, detail=f"report_id {report_id} não encontrado.")
        new_file_name = row[0]
        conn_op.commit()
        cur_op.close()
        conn_op.close()

        # Limpar erros deste report no etl_logs_pdfs
        try:
            conn_pipe = get_pipeline_connection()
            cur_pipe = conn_pipe.cursor()
            cur_pipe.execute(
                "DELETE FROM etl_logs_pdfs WHERE report_id = %s AND status = 'error'",
                (report_id,)
            )
            conn_pipe.commit()
            cur_pipe.close()
            conn_pipe.close()
        except Exception:
            pass

        # Remover PDF do bucket bronze-unstructured para forçar nova transferência
        try:
            s3 = get_s3()
            s3.delete_object(Bucket=BUCKET_UNSTRUCTURED, Key=new_file_name)
        except Exception:
            pass

        return {"message": "Relatório atualizado. Será reingerido na próxima execução da pipeline."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/op_report/{report_id}")
def delete_op_report(report_id: int):
    """Apaga relatório (CASCADE em op_data) apenas se ainda não processado por etl_pdfs."""
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT created_at FROM op_report WHERE report_id = %s", (report_id,))
        report = cur.fetchone()
        if not report:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Relatório não encontrado.")

        cur.execute("SELECT last_run FROM etl_data WHERE process_name = 'etl_pdfs'")
        etl_pdfs = cur.fetchone()
        if etl_pdfs and etl_pdfs["last_run"] and report["created_at"] <= etl_pdfs["last_run"]:
            cur.close(); conn.close()
            raise HTTPException(
                status_code=409,
                detail="Este relatório já foi processado pelo pipeline de PDFs e não pode ser apagado diretamente."
            )

        cur.execute("""
            SELECT COUNT(*) FROM op_data od
            JOIN etl_data ed ON ed.process_name = 'etl_dados'
            WHERE od.report_id = %s AND od.created_at <= ed.last_run
        """, (report_id,))
        if cur.fetchone()["count"] > 0:
            cur.close(); conn.close()
            raise HTTPException(
                status_code=409,
                detail="Este relatório tem dados já processados pelo pipeline de dados e não pode ser apagado diretamente."
            )

        cur.execute("DELETE FROM op_report WHERE report_id = %s", (report_id,))
        conn.commit()
        cur.close(); conn.close()
        return {"deleted": report_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/extract_functions")
def get_extract_functions():
    auto = _load_auto_store()
    all_fns = {
        **{n: FUNCTION_FILE_TYPE.get(n, "") for n in _EXTRACT_FUNCTIONS},
        **{n: e.get("file_type", "") for n, e in auto.items()},
    }
    return [{"name": name, "file_type": ft} for name, ft in all_fns.items()]


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
                    SELECT DISTINCT i.indicator_sk, i.indicator_code, i.indicator_name, i.source_system
                    FROM dim_indicator i
                    JOIN fact_values f ON f.indicator_sk = i.indicator_sk
                    WHERE f.report_id = ANY(%s)
                    ORDER BY i.indicator_name;
                """, (report_ids,))
                rows = [dict(r) | {"source_code": source_code} for r in cur_dw.fetchall()]
        else:
            cur_dw.execute("""
                SELECT DISTINCT ON (i.indicator_sk) i.indicator_sk, i.indicator_code, i.indicator_name, i.source_system, f.report_id
                FROM dim_indicator i
                JOIN fact_values f ON f.indicator_sk = i.indicator_sk
                ORDER BY i.indicator_sk;
            """)
            dw_rows = cur_dw.fetchall()
            if dw_rows:
                rids = list({r["report_id"] for r in dw_rows})
                cur_op.execute("SELECT report_id, source_code FROM op_report WHERE report_id = ANY(%s)", (rids,))
                src_map = {r["report_id"]: r["source_code"] for r in cur_op.fetchall()}
                rows = [{"indicator_sk": r["indicator_sk"], "indicator_code": r["indicator_code"], "indicator_name": r["indicator_name"], "source_system": r["source_system"], "source_code": src_map.get(r["report_id"])} for r in dw_rows]
            else:
                rows = []

        cur_dw.close(); conn_dw.close()
        cur_op.close(); conn_op.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fact_values")
def get_fact_values(indicator_sk: int):
    try:
        conn = get_warehouse_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT c.location_code, c.name AS location_name, d.year, f.value
            FROM fact_values f
            JOIN dim_location c ON f.location_sk = c.location_sk
            JOIN dim_indicator i ON f.indicator_sk = i.indicator_sk
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE i.indicator_sk = %s
            ORDER BY d.year ASC, c.name ASC;
        """, (indicator_sk,))
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
def get_dashboard(indicator_name: str, year: int, report_id: int = None):
    try:
        conn = get_warehouse_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if report_id is not None:
            cur.execute("""
                SELECT dl.name AS location_name, fv.value
                FROM fact_values fv
                JOIN dim_indicator di ON fv.indicator_sk = di.indicator_sk
                JOIN dim_location dl ON fv.location_sk = dl.location_sk
                JOIN dim_date dd ON fv.date_id = dd.date_id
                WHERE TRIM(REPLACE(REPLACE(di.indicator_name, E'\n', ' '), E'\r', '')) = %s
                  AND dd.year = %s AND fv.report_id = %s
                ORDER BY dl.name;
            """, (indicator_name.strip(), year, report_id))
        else:
            cur.execute(
                """SELECT location_name, value FROM view
                   WHERE TRIM(REPLACE(REPLACE(indicator_name, E'\n', ' '), E'\r', '')) = %s
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
def get_dashboard_filters(indicator_name: str = None, report_id: int = None):
    try:
        conn = get_warehouse_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if report_id is not None:
            cur.execute("""
                SELECT DISTINCT REPLACE(REPLACE(di.indicator_name, E'\n', ' '), E'\r', '') AS indicator_name
                FROM fact_values fv
                JOIN dim_indicator di ON fv.indicator_sk = di.indicator_sk
                WHERE fv.report_id = %s ORDER BY 1;
            """, (report_id,))
        else:
            cur.execute("""
                SELECT DISTINCT REPLACE(REPLACE(indicator_name, E'\n', ' '), E'\r', '') AS indicator_name
                FROM view ORDER BY 1;
            """)
        indicators = [r["indicator_name"].strip() for r in cur.fetchall()]

        if indicator_name and report_id is not None:
            cur.execute("""
                SELECT DISTINCT dd.year
                FROM fact_values fv
                JOIN dim_indicator di ON fv.indicator_sk = di.indicator_sk
                JOIN dim_date dd ON fv.date_id = dd.date_id
                WHERE TRIM(REPLACE(REPLACE(di.indicator_name, E'\n', ' '), E'\r', '')) = %s
                  AND fv.report_id = %s
                ORDER BY dd.year DESC;
            """, (indicator_name.strip(), report_id))
        elif indicator_name:
            cur.execute("""
                SELECT DISTINCT year FROM view
                WHERE TRIM(REPLACE(REPLACE(indicator_name, E'\n', ' '), E'\r', '')) = %s
                ORDER BY year DESC;
            """, (indicator_name.strip(),))
        elif report_id is not None:
            cur.execute("""
                SELECT DISTINCT dd.year
                FROM fact_values fv
                JOIN dim_date dd ON fv.date_id = dd.date_id
                WHERE fv.report_id = %s ORDER BY dd.year DESC;
            """, (report_id,))
        else:
            cur.execute("SELECT DISTINCT year FROM view ORDER BY year DESC;")

        years = [r["year"] for r in cur.fetchall()]
        cur.close()
        conn.close()
        return {"indicators": indicators, "years": years}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dashboard/timeseries")
def get_dashboard_timeseries(indicator_name: str, countries: str, report_id: int = None):
    try:
        country_list = [c.strip() for c in countries.split(",") if c.strip()]
        if not country_list:
            raise HTTPException(status_code=400, detail="Pelo menos um país deve ser fornecido.")
        conn = get_warehouse_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if report_id is not None:
            cur.execute("""
                SELECT dl.name AS location_name, dd.year, fv.value
                FROM fact_values fv
                JOIN dim_indicator di ON fv.indicator_sk = di.indicator_sk
                JOIN dim_location dl ON fv.location_sk = dl.location_sk
                JOIN dim_date dd ON fv.date_id = dd.date_id
                WHERE TRIM(REPLACE(REPLACE(di.indicator_name, E'\n', ' '), E'\r', '')) = %s
                  AND dl.name = ANY(%s) AND fv.report_id = %s
                ORDER BY dl.name, dd.year ASC;
            """, (indicator_name.strip(), country_list, report_id))
        else:
            cur.execute("""
                SELECT location_name, year, value FROM view
                WHERE TRIM(REPLACE(REPLACE(indicator_name, E'\n', ' '), E'\r', '')) = %s
                  AND location_name = ANY(%s)
                ORDER BY location_name, year ASC;
            """, (indicator_name.strip(), country_list))
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
            SELECT id, file_id, NULL::integer AS report_id, file_name, step, status, error_message, log_time, 'dados' AS pipeline FROM etl_logs_dados
            UNION ALL
            SELECT id, NULL AS file_id, report_id, file_name, step, status, error_message, log_time, 'pdfs' AS pipeline FROM etl_logs_pdfs
            ORDER BY log_time DESC LIMIT 500
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/etl_data")
def get_etl_data():
    try:
        conn = get_pipeline_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM etl_data ORDER BY process_name")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/etl_logs/dados")
def get_etl_logs_dados():
    try:
        conn = get_pipeline_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM etl_logs_dados ORDER BY log_time DESC LIMIT 500")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/etl_logs/pdfs")
def get_etl_logs_pdfs():
    try:
        conn = get_pipeline_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT report_id, file_name, step, status, error_message, log_time
            FROM etl_logs_pdfs
            ORDER BY log_time DESC
            LIMIT 500
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


@app.post("/chat-data")
def chat_data(body: ChatIn):
    if not body.question.strip():
        raise HTTPException(status_code=400, detail="A pergunta não pode estar vazia.")
    try:
        answer = chatbot_sql(body.question)
        return {"answer": answer, "sources": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
