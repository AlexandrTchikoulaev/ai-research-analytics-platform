from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, date
import psycopg2
import psycopg2.extras
import subprocess
import sys
import os
import re
import json
import threading
import boto3

# Importar mapeamentos do silver_functions
sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "Pipeline")))
from silver_functions import EXTRACT_FUNCTIONS as _EXTRACT_FUNCTIONS
from silver_function_generator import generate_and_validate as _generate_and_validate

from chat_data import chatbot_sql
from chat_reports import query_rag

# ── Configurações ─────────────────────────────────────────
_DB_BASE = {"host": "localhost", "port": 5433, "user": "projeto_utilizador", "password": "projeto"}

DB_WAREHOUSE   = {**_DB_BASE, "database": "warehouse_db"}
DB_OPERATIONAL = {**_DB_BASE, "database": "gestao_db"}
DB_PIPELINE    = {**_DB_BASE, "database": "gestao_db"}

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

BUCKET_UNSTRUCTURED = "bronze-unstructured"
BUCKET_RAW = "bronze"
BUCKET_THUMBNAILS = "thumbnails"


_HERE = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DADOS_SCRIPT  = os.path.normpath(os.path.join(_HERE, "..", "Pipeline", "pipeline_data.py"))
PIPELINE_PDFS_SCRIPT   = os.path.normpath(os.path.join(_HERE, "..", "Pipeline Unstructured", "pipeline_reports.py"))
RESET_PIPELINE_SCRIPT  = os.path.normpath(os.path.join(_HERE, "..", "..", "Extra", "Codes", "reset_pipeline.py"))

# A pipeline de PDFs precisa do ambiente projeto_final (pdfplumber, langchain, etc.)
# Procura python.exe nos locais mais comuns de instalação conda/venv
def _find_pdfs_python() -> str:
    # Testa com minio+langchain_community para garantir que é o env projetofinal,
    # não o env base (que pode ter pdfplumber mas não as dependências do pipeline).
    _CHECK = "import minio; import langchain_community; import pdfplumber; import langchain_ollama; from sqlalchemy.pool import NullPool"
    candidates = [
        # Caminhos absolutos do ambiente projetofinal — verificados primeiro
        r"C:\Users\Alexandr\miniconda3\envs\projetofinal\python.exe",
        r"C:\Users\Alexandr\anaconda3\envs\projetofinal\python.exe",
        # Subambiente projetofinal a partir do diretório base do conda
        os.path.join(os.path.dirname(sys.executable), "envs", "projetofinal", "python.exe"),
        os.path.join(os.path.dirname(sys.executable), "..", "envs", "projetofinal", "python.exe"),
        # Mesmo executável atual (último recurso)
        sys.executable,
    ]
    for path in candidates:
        path = os.path.normpath(path)
        if not os.path.exists(path):
            continue
        try:
            import subprocess as _sp
            result = _sp.run([path, "-c", _CHECK], capture_output=True, timeout=15)
            if result.returncode == 0:
                return path
        except Exception:
            continue
    return sys.executable

_PDFS_PYTHON = _find_pdfs_python()

# ── Estado da pipeline de dados ───────────────────────────
_dados_state_lock = threading.Lock()
_dados_running    = False
_dados_pending    = False

_REPORTS_DATA_DIR = os.path.normpath(os.path.join(_HERE, "..", "..", "Reports", "Data"))
_REPORTS_PDFS_DIR = os.path.normpath(os.path.join(_HERE, "..", "..", "Reports", "PDFs"))

# Buffers em memória para o output das pipelines (acesso thread-safe via lock)
_dados_log_lock   = threading.Lock()
_dados_log_lines: list = []
_dados_log_run_id: int = 0

_pdfs_log_lock    = threading.Lock()
_pdfs_log_lines: list = []
_pdfs_log_run_id: int = 0


def _run_dados_loop():
    """Worker em background: corre pipeline_data.py; repete se ficou execução pendente."""
    global _dados_running, _dados_pending, _dados_log_run_id
    script_dir = os.path.dirname(PIPELINE_DADOS_SCRIPT)
    while True:
        with _dados_log_lock:
            _dados_log_run_id += 1
            _dados_log_lines.clear()
        try:
            _env = os.environ.copy()
            _env["OPENBLAS_NUM_THREADS"] = "1"
            _env["OMP_NUM_THREADS"] = "1"
            proc = subprocess.Popen(
                [sys.executable, "-u", PIPELINE_DADOS_SCRIPT],
                cwd=script_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=subprocess.CREATE_NO_WINDOW,
                env=_env,
            )
            for raw in proc.stdout:
                line = raw.rstrip("\n").rstrip("\r")
                if line:
                    with _dados_log_lock:
                        _dados_log_lines.append(line)
            proc.wait()
            status = "✓ Concluído" if proc.returncode == 0 else f"✗ Erro (código {proc.returncode})"
            with _dados_log_lock:
                _dados_log_lines.append(status)
        except Exception as e:
            with _dados_log_lock:
                _dados_log_lines.append(f"[ERRO ao lançar] {e}")
        with _dados_state_lock:
            if _dados_pending:
                _dados_pending = False
            else:
                _dados_running = False
                break


def notify_pipeline_dados():
    """Dispara a pipeline de dados em background após uma inserção.
    Se já estiver a correr, marca como pendente para reexecutar ao terminar."""
    global _dados_running, _dados_pending
    with _dados_state_lock:
        if _dados_running:
            _dados_pending = True
            return
        _dados_running = True
    threading.Thread(target=_run_dados_loop, daemon=True).start()


# ── Estado da pipeline de PDFs ────────────────────────────
_pdfs_state_lock = threading.Lock()
_pdfs_running    = False
_pdfs_pending    = False


_PDFS_ERROR_LOG = os.path.normpath(os.path.join(
    _HERE, "..", "..", "Reports", "PDFs", "pipeline_pdfs_stderr.log"
))
_PDFS_STEP_FILE = os.path.normpath(os.path.join(
    _HERE, "..", "..", "Reports", "PDFs", "meta", "pipeline_pdfs_status.json"
))


def _run_pdfs_loop():
    """Worker em background: corre pipeline_reports.py; repete se ficou execução pendente."""
    global _pdfs_running, _pdfs_pending, _pdfs_log_run_id
    script_dir = os.path.dirname(PIPELINE_PDFS_SCRIPT)
    while True:
        with _pdfs_log_lock:
            _pdfs_log_run_id += 1
            _pdfs_log_lines.clear()
            _pdfs_log_lines.append(f"[INFO] A usar Python: {_PDFS_PYTHON}")
        try:
            proc = subprocess.Popen(
                [_PDFS_PYTHON, "-u", PIPELINE_PDFS_SCRIPT],
                cwd=script_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            for raw in proc.stdout:
                line = raw.rstrip("\n").rstrip("\r")
                if line:
                    with _pdfs_log_lock:
                        _pdfs_log_lines.append(line)
            proc.wait()
            status = "✓ Concluído" if proc.returncode == 0 else f"✗ Erro (código {proc.returncode})"
            with _pdfs_log_lock:
                _pdfs_log_lines.append(status)
        except Exception as e:
            with _pdfs_log_lock:
                _pdfs_log_lines.append(f"[ERRO ao lançar] {e}")
        with _pdfs_state_lock:
            if _pdfs_pending:
                _pdfs_pending = False
            else:
                _pdfs_running = False
                break


def notify_pipeline_pdfs():
    """Dispara a pipeline de PDFs em background após uma inserção.
    Se já estiver a correr, marca como pendente para reexecutar ao terminar."""
    global _pdfs_running, _pdfs_pending
    with _pdfs_state_lock:
        if _pdfs_running:
            _pdfs_pending = True
            return
        _pdfs_running = True
    threading.Thread(target=_run_pdfs_loop, daemon=True).start()


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


def _make_thumbnail(pdf_bytes: bytes) -> bytes | None:
    """Converte a primeira página de um PDF em JPEG. Devolve None em caso de falha."""
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if len(doc) == 0:
            doc.close()
            return None
        pix = doc[0].get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False)
        img_bytes = pix.tobytes("jpeg")
        doc.close()
        return img_bytes
    except Exception:
        return None


def _cache_thumbnail(s3, report_id: int, img_bytes: bytes):
    """Guarda thumbnail no MinIO. Falha silenciosamente."""
    try:
        ensure_bucket(s3, BUCKET_THUMBNAILS)
        s3.put_object(
            Bucket=BUCKET_THUMBNAILS,
            Key=f"{report_id}.jpg",
            Body=img_bytes,
            ContentType="image/jpeg",
        )
    except Exception:
        pass


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


@app.on_event("startup")
def _create_mapping_table():
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS source_function_mapping (
                source_code          TEXT PRIMARY KEY,
                extract_function     TEXT,
                ai_extract_function  TEXT
            )
        """)
        # Migrações de schema
        cur.execute("ALTER TABLE op_data DROP COLUMN IF EXISTS extract_function")
        cur.execute("ALTER TABLE op_data ADD COLUMN IF NOT EXISTS auto_generate BOOLEAN NOT NULL DEFAULT TRUE")
        cur.execute("ALTER TABLE op_data ADD COLUMN IF NOT EXISTS transform_fn_name TEXT")
        cur.execute("ALTER TABLE op_data ADD COLUMN IF NOT EXISTS transform_fn_source TEXT")
        cur.execute("ALTER TABLE source_function_mapping ADD COLUMN IF NOT EXISTS ai_extract_function TEXT")
        cur.execute("ALTER TABLE source_function_mapping ADD COLUMN IF NOT EXISTS generation_hint TEXT")
        cur.execute("ALTER TABLE source_function_mapping ALTER COLUMN extract_function DROP NOT NULL")

        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass
    _seed_generation_hints()


_SEED_HINTS = {
    "YCELP, CIESIN": (
        "CSV file from the Environmental Performance Index (EPI). "
        "Columns present: 'iso' (3-letter ISO-3 country code — THIS is location_code), "
        "'country' (full country name — ignore for location_code), possibly 'code' (ignore). "
        "Then one or more value columns named INDICATOR.raw.YEAR (e.g. COE.raw.2022, BER.raw.2010). "
        "CRITICAL: location_code MUST come from the 'iso' column, NOT 'code' or 'country'. "
        "Extract indicator_code from the part before the first dot (e.g. 'BER' from 'BER.raw.2022'). "
        "year = 4-digit suffix of each column name. "
        "Use re.match(r'^.+\\.raw\\.\\d{4}$', col) to find value columns. "
        "Melt all value columns to get one row per (iso, year, value)."
    ),
    "IMF": (
        "JSON file from the IMF (International Monetary Fund). "
        "Structure: {\"indicators\": {\"CODE\": {\"label\": \"Full Name\", ...}}, "
        "\"values\": {\"CODE\": {\"ISO3\": {\"YEAR_STR\": value_or_null}}}}. "
        "ALL keys are strings — years are string keys, convert with int(yr). "
        "Values at any level can be null — always guard .items() calls with isinstance(x, dict). "
        "location_code = ISO3 key, indicator_code = CODE key, "
        "indicator_name = indicators[CODE]['label'], year = int(yr), value = float(v). "
        "Drop rows where value is None/null."
    ),
    "Cato Institute": (
        "Excel file from the Cato Institute Human Freedom Index (146 columns). "
        "Read with default header=0. First 4 columns are Unnamed: 0 (year), Unnamed: 1 (ISO3 code), "
        "Unnamed: 2 (country name), Unnamed: 3 (region) — these are the meta columns. "
        "All remaining columns are indicator names (e.g. 'HUMAN FREEDOM', 'Ai Procedural Justice', ...). "
        "Skip columns where the name: starts with 'Unnamed:', starts with 'data' (pandas renames duplicates to data.1, data.2...), "
        "contains 'RANK' or 'QUARTILE', or equals 'Rank'. "
        "For each data row: location_code = Unnamed: 1 (must be 3-letter uppercase alpha), "
        "year = int(Unnamed: 0), indicator_code = indicator_name = column name (stripped). "
        "Extract ALL non-skip columns as separate rows. Drop rows where value is NaN."
    ),
    "Portulans": (
        "Excel file from the Network Readiness Index (Portulans Institute). "
        "Has 5 header rows before data: row index 2 = readable indicator names, "
        "row index 4 = machine codes (ISO3Code, NRI.score, 1.score, 1.1.score, ...). "
        "Data starts at row index 5. "
        "Use row index 4 as column headers (df.columns = data.iloc[4]). "
        "location_code = 'ISO3Code' column. "
        "Value columns = all columns whose name contains 'score' (case-insensitive), excluding meta columns. "
        "indicator_code = column name with '.score' suffix removed. "
        "indicator_name = corresponding value from row index 2. "
        "year is FIXED = 2025 (no year column in this file)."
    ),
    "The Heritage Foundation": (
        "Excel file from the Heritage Foundation Index of Economic Freedom. "
        "Row 0 is a title row ('COMPONENT SCORES') — real column headers are in row 1. "
        "Use data.iloc[1] as headers, data.iloc[2:] as rows. "
        "Columns: 'Country' (full country name — NOT ISO code), 'Region', then indicator columns. "
        "CRITICAL: 'Country' contains names like 'Portugal', 'United States' — must convert to ISO3 "
        "using pycountry: pycountry.countries.search_fuzzy(name)[0].alpha_3. Skip if not found. "
        "year is FIXED = 2026. Skip columns starting with 'Unnamed'."
    ),
    "Fraser Institute": (
        "Excel file from the Fraser Institute Economic Freedom of the World. "
        "Has 4 metadata rows before data — real column headers are at row index 3. "
        "Use data.iloc[3] as headers, data.iloc[4:] as rows. "
        "Key columns: 'Year' (int), 'ISO Code 3' (3-letter ISO — THIS is location_code), "
        "'Countries', 'Rank', 'Quartile', 'World Bank Region', "
        "'World Bank Current Income Classification, 1990-Present'. "
        "Value columns = all other columns except meta columns above and columns containing 'Rank' or named 'nan'. "
        "indicator_code = column name (stripped), indicator_name = column name. "
        "Skip rows where 'ISO Code 3' or 'Year' are NaN."
    ),
    "WEF": (
        "Excel file from the WEF Travel & Tourism Development Index, sheet 'Dataset'. "
        "Read with header=None. Header row is at row index 3. "
        "Column index 9 = 'Attribute'. Columns from index 10 onwards = ISO3 country codes. "
        "Only keep rows where column 9 (Attribute) == 'Score' (case-sensitive). "
        "Column index 6 = indicator_code, column index 7 = indicator_name. "
        "year is FIXED = 2024. "
        "Each row gives one indicator score for all countries — iterate columns 10+ to get "
        "(location_code=country_iso, indicator_code, indicator_name, year=2024, value=float)."
    ),
}


def _seed_generation_hints():
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        for src, hint in _SEED_HINTS.items():
            cur.execute("""
                INSERT INTO source_function_mapping (source_code, generation_hint)
                VALUES (%s, %s)
                ON CONFLICT (source_code) DO UPDATE
                    SET generation_hint = EXCLUDED.generation_hint
            """, (src, hint))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[WARN] _seed_generation_hints falhou: {e}")


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


class OpDataIn(BaseModel):
    report_id: int
    file_url: str = ""
    file_name: str = ""
    auto_generate: bool = True


class OpDataPatch(BaseModel):
    report_id: Optional[int] = None
    file_url: Optional[str] = None
    file_name: Optional[str] = None


class OpDataSimplePatch(BaseModel):
    file_name: Optional[str] = None


class ReportPatch(BaseModel):
    report_url: Optional[str] = None
    file_name: Optional[str] = None
    source_code: Optional[str] = None
    publication_date: Optional[date] = None
    area_tematica: Optional[str] = None
    estado: Optional[str] = None
    palavras_chave: Optional[str] = None
    resumo: Optional[str] = None



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
        notify_pipeline_pdfs()
        return {"report_id": report_id, "message": "Relatório inserido com sucesso."}
    except Exception as e:
        if getattr(e, 'pgcode', None) == '23505':
            raise HTTPException(status_code=409, detail="Já existe um relatório com este URL.")
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

    img = _make_thumbnail(content)
    if img:
        _cache_thumbnail(s3, report_id, img)

    notify_pipeline_pdfs()
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
            msg = "URL duplicado" if getattr(e, 'pgcode', None) == '23505' else str(e)
            errors.append({"index": i, "file_name": item.get("file_name", "?"), "error": msg})

    conn.commit()
    cur.close()
    conn.close()
    if inserted > 0:
        notify_pipeline_pdfs()
    return {"inserted": inserted, "errors": errors}


@app.post("/op_report/upload-files", status_code=201)
async def upload_report_files(files: List[UploadFile] = File(...)):
    """Insere múltiplos PDFs sem metadados (todos os campos null exceto file_name)."""
    conn = get_operational_connection()
    cur = conn.cursor()
    s3 = get_s3()
    ensure_bucket(s3, BUCKET_UNSTRUCTURED)
    results = []
    errors = []

    for f in files:
        content = await f.read()
        if content[:4] != b"%PDF":
            errors.append({"file_name": f.filename, "error": "Não é um PDF válido."})
            continue
        try:
            cur.execute(
                "INSERT INTO op_report (file_name) VALUES (%s) RETURNING report_id;",
                (f.filename,)
            )
            report_id = cur.fetchone()[0]
            conn.commit()

            s3.put_object(
                Bucket=BUCKET_UNSTRUCTURED,
                Key=f.filename,
                Body=content,
                ContentType="application/pdf",
                Metadata={"report_id": str(report_id), "file_name": f.filename},
            )

            img = _make_thumbnail(content)
            if img:
                _cache_thumbnail(s3, report_id, img)

            results.append({"report_id": report_id, "file_name": f.filename})
        except Exception as e:
            conn.rollback()
            errors.append({"file_name": f.filename, "error": str(e)})

    cur.close()
    conn.close()
    if results:
        notify_pipeline_pdfs()
    return {"inserted": len(results), "results": results, "errors": errors}


@app.post("/op_report/batch-links", status_code=201)
async def batch_report_links(urls: list[str]):
    """Insere múltiplos relatórios com apenas report_url, restantes campos null."""
    conn = get_operational_connection()
    cur = conn.cursor()
    inserted = 0
    errors = []

    for i, url in enumerate(urls):
        sp = f"sp_{i}"
        try:
            cur.execute(f"SAVEPOINT {sp}")
            cur.execute("INSERT INTO op_report (report_url) VALUES (%s)", (url,))
            cur.execute(f"RELEASE SAVEPOINT {sp}")
            inserted += 1
        except Exception as e:
            cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            msg = "URL duplicado" if getattr(e, 'pgcode', None) == '23505' else str(e)
            errors.append({"index": i, "url": url, "error": msg})

    conn.commit()
    cur.close()
    conn.close()
    if inserted > 0:
        notify_pipeline_pdfs()
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
            INSERT INTO op_data (report_id, file_url, file_name, auto_generate)
            VALUES (%s, %s, %s, %s)
            RETURNING file_id;
        """, (data.report_id, data.file_url or None, data.file_name or "", data.auto_generate))
        file_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        notify_pipeline_dados()
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
    auto_generate: bool = Form(True),
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
    original_name = file.filename or ""
    try:
        cur.execute("""
            INSERT INTO op_data (report_id, file_url, file_name, auto_generate)
            VALUES (%s, NULL, %s, %s)
            RETURNING file_id;
        """, (report_id, original_name, auto_generate))
        file_id = cur.fetchone()[0]
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
    try:
        s3.put_object(
            Bucket=BUCKET_RAW,
            Key=str(file_id),
            Body=content,
            Metadata={
                "report_id": str(report_id),
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao guardar no MinIO: {e}")

    notify_pipeline_dados()
    return {"file_id": file_id, "message": "Ficheiro carregado e registado com sucesso."}




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
            report_id     = item.get("report_id")
            file_url      = item.get("file_url") or None
            auto_generate = bool(item.get("auto_generate", True))
            if not report_id:
                raise ValueError("report_id é obrigatório")

            cur.execute("SELECT 1 FROM op_report WHERE report_id = %s", (report_id,))
            if not cur.fetchone():
                raise ValueError(f"report_id {report_id} não existe")

            cur.execute("""
                INSERT INTO op_data (report_id, file_url, auto_generate)
                VALUES (%s, %s, %s)
            """, (report_id, file_url, auto_generate))
            cur.execute(f"RELEASE SAVEPOINT {sp}")
            inserted += 1
        except Exception as e:
            cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            errors.append({"index": i, "error": str(e)})

    conn.commit()
    cur.close()
    conn.close()
    if inserted > 0:
        notify_pipeline_dados()
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


def _save_to_auto_store(name: str, code: str):
    from datetime import datetime
    import pandas as _pd
    store = _load_auto_store()
    store[name] = {
        "code":       code,
        "created_at": datetime.now().isoformat(),
    }
    with open(_AUTO_STORE, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2)
    # Carregar a função em memória para que o registo /extract_functions a inclua
    try:
        ns = {"pd": _pd}
        exec(compile(code, "<auto>", "exec"), ns)
        fn = ns.get(name)
        if callable(fn):
            _EXTRACT_FUNCTIONS[name] = fn
    except Exception:
        _EXTRACT_FUNCTIONS[name] = None


@app.post("/generate_function")
async def generate_function(
    file: UploadFile = File(...),
):
    """Gera automaticamente uma função de transformação silver via Ollama."""
    content = await file.read()
    result = _generate_and_validate(content)

    if result["generated"] and result["valid"]:
        try:
            _save_to_auto_store(result["function_name"], result["code"])
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
    if not body.url.strip():
        raise HTTPException(status_code=400, detail="URL é obrigatório.")

    import requests as _req
    try:
        resp = _req.get(body.url.strip(), timeout=30, stream=True)
        resp.raise_for_status()
        content = b""
        for chunk in resp.iter_content(chunk_size=8192):
            content += chunk
            if len(content) >= 512 * 1024:
                break
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao descarregar URL: {e}")

    result = _generate_and_validate(content)

    if result["generated"] and result["valid"]:
        try:
            _save_to_auto_store(result["function_name"], result["code"])
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
                   area_tematica, estado, palavras_chave, resumo, pipeline_status
            FROM op_report ORDER BY report_id DESC;
        """)
        reports = cur.fetchall()

        cur.execute("SELECT DISTINCT report_id FROM op_data WHERE pipeline_status NOT IN ('PENDING', 'FAILED')")
        data_processed_ids = {r["report_id"] for r in cur.fetchall()}

        cur.close()
        conn.close()

        result = []
        for r in reports:
            row = dict(r)
            pdf_done  = r["pipeline_status"] not in ("PENDING", "FAILED")
            data_done = r["report_id"] in data_processed_ids
            row["can_delete"] = not pdf_done and not data_done
            row["url_locked"] = r["pipeline_status"] in ("BRONZE_OK", "DONE")
            result.append(row)

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/op_data")
def get_op_data():
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT d.file_id, d.report_id, d.file_url,
                   r.source_code, d.file_name,
                   r.file_name AS report_name,
                   CASE
                     WHEN d.pipeline_status IN ('PENDING', 'FAILED') THEN TRUE
                     ELSE FALSE
                   END AS can_delete
            FROM op_data d
            LEFT JOIN op_report r  ON r.report_id  = d.report_id
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
        cur.execute("""
            SELECT d.file_id, d.file_name, d.report_id, d.file_url,
                   r.file_name AS report_name, r.source_code
            FROM op_data d
            LEFT JOIN op_report r ON r.report_id = d.report_id
            WHERE d.file_id = %s
        """, (file_id,))
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


@app.patch("/op_data/{file_id}/edit")
def patch_op_data_simple(file_id: int, data: OpDataSimplePatch):
    """Edita o nome do ficheiro; repõe status para reprocessamento."""
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE op_data
            SET file_name        = COALESCE(%s, file_name),
                pipeline_status  = 'PENDING',
                pipeline_error   = NULL
            WHERE file_id = %s
            RETURNING file_id
        """, (data.file_name or None, file_id))
        if not cur.fetchone():
            conn.rollback()
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail=f"file_id {file_id} não encontrado.")
        cur.execute("DELETE FROM etl_logs_dados WHERE file_id = %s", (str(file_id),))
        conn.commit()
        cur.close(); conn.close()
        return {"message": "Ficheiro atualizado. Será reinserido na próxima execução da pipeline."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/op_data/{file_id}")
def patch_op_data(file_id: int, data: OpDataPatch):
    """Edita os campos de um ficheiro em op_data e repõe status para ser reprocessado na próxima pipeline."""
    try:
        conn_op = get_operational_connection()
        cur_op = conn_op.cursor()

        cur_op.execute("""
            UPDATE op_data
            SET report_id        = COALESCE(%s, report_id),
                file_url         = %s,
                file_name        = COALESCE(%s, file_name),
                pipeline_status  = 'PENDING',
                pipeline_error   = NULL
            WHERE file_id = %s
            RETURNING report_id, file_url
        """, (data.report_id, data.file_url or None, data.file_name or None, file_id))

        row = cur_op.fetchone()
        if not row:
            conn_op.rollback()
            cur_op.close(); conn_op.close()
            raise HTTPException(status_code=404, detail=f"file_id {file_id} não encontrado.")

        new_report_id, new_file_url = row
        conn_op.commit()
        cur_op.close()
        conn_op.close()

        # Remover logs de erro deste file_id para que o bronze não o coloque na blacklist
        try:
            conn_pipe = get_pipeline_connection()
            cur_pipe = conn_pipe.cursor()
            cur_pipe.execute(
                "DELETE FROM etl_logs_dados WHERE file_id = %s",
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
            new_meta = {
                "report_id": str(new_report_id) if new_report_id is not None else "",
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


# Steps que acontecem depois de o ficheiro já estar no MinIO bronze
_BRONZE_DONE_STEPS = {"validate_bronze", "transform", "validate_silver", "load"}


@app.post("/op_data/{file_id}/retry")
def retry_op_data(file_id: int):
    """Repõe um ficheiro FAILED para reprocessamento e aciona o pipeline."""
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(
            "SELECT pipeline_status, file_url FROM op_data WHERE file_id = %s",
            (file_id,)
        )
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail=f"file_id {file_id} não encontrado.")
        if row["pipeline_status"] not in ("FAILED", "PENDING"):
            cur.close(); conn.close()
            raise HTTPException(
                status_code=409,
                detail=f"Ficheiro não está em estado FAILED (status atual: {row['pipeline_status']})."
            )

        # Determinar o step onde falhou para saber de onde recomeçar
        cur.execute(
            "SELECT step FROM etl_logs_dados WHERE file_id = %s ORDER BY log_time DESC LIMIT 1",
            (str(file_id),)
        )
        log_row = cur.fetchone()
        failed_step = log_row["step"] if log_row else None

        # Se já passou pelo bronze, retoma daí; senão recomeça do início
        if failed_step in _BRONZE_DONE_STEPS or (not row["file_url"] and failed_step):
            reset_status = "BRONZE_OK"
        else:
            reset_status = "PENDING"

        cur.execute(
            "UPDATE op_data SET pipeline_status = %s, pipeline_error = NULL WHERE file_id = %s",
            (reset_status, file_id)
        )
        cur.execute("DELETE FROM etl_logs_dados WHERE file_id = %s", (str(file_id),))
        conn.commit()
        cur.close(); conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    notify_pipeline_dados()
    return {"file_id": file_id, "reset_to": reset_status, "message": "Retry iniciado."}


@app.post("/op_data/retry_failed")
def retry_all_failed():
    """Repõe todos os ficheiros FAILED para reprocessamento e aciona o pipeline."""
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(
            "SELECT file_id, file_url FROM op_data WHERE pipeline_status = 'FAILED'"
        )
        failed = cur.fetchall()
        if not failed:
            cur.close(); conn.close()
            return {"retried": 0, "message": "Nenhum ficheiro em FAILED."}

        # Para cada ficheiro, determinar o step mais recente
        resets = []
        for r in failed:
            fid = r["file_id"]
            cur.execute(
                "SELECT step FROM etl_logs_dados WHERE file_id = %s ORDER BY log_time DESC LIMIT 1",
                (str(fid),)
            )
            log_row = cur.fetchone()
            failed_step = log_row["step"] if log_row else None
            if failed_step in _BRONZE_DONE_STEPS or (not r["file_url"] and failed_step):
                reset_status = "BRONZE_OK"
            else:
                reset_status = "PENDING"
            resets.append((reset_status, fid))

        for reset_status, fid in resets:
            cur.execute(
                "UPDATE op_data SET pipeline_status = %s, pipeline_error = NULL WHERE file_id = %s",
                (reset_status, fid)
            )
            cur.execute("DELETE FROM etl_logs_dados WHERE file_id = %s", (str(fid),))

        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    notify_pipeline_dados()
    return {"retried": len(resets), "message": f"{len(resets)} ficheiro(s) repostos e pipeline acionada."}


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
    """Edita campos de op_report, repõe status e limpa erros para reprocessamento."""
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
                pipeline_status  = 'PENDING',
                pipeline_error   = NULL
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
                "DELETE FROM etl_logs_pdfs WHERE report_id = %s",
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
        if getattr(e, 'pgcode', None) == '23505':
            raise HTTPException(status_code=409, detail="Já existe um relatório com este URL.")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/op_data/{file_id}")
def delete_op_data(file_id: int):
    """Apaga ficheiro de dados apenas se ainda não chegou ao DW."""
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT file_id, report_id, pipeline_status FROM op_data WHERE file_id = %s", (file_id,))
        file_row = cur.fetchone()
        if not file_row:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail=f"file_id {file_id} não encontrado.")

        if file_row["pipeline_status"] not in ("PENDING", "FAILED"):
            # Permite apagar se não chegou nenhuma linha ao warehouse
            try:
                conn_dw = get_warehouse_connection()
                cur_dw  = conn_dw.cursor()
                cur_dw.execute("SELECT COUNT(*) FROM fact_values WHERE report_id = %s", (file_row["report_id"],))
                dw_count = cur_dw.fetchone()[0]
                cur_dw.close(); conn_dw.close()
            except Exception:
                dw_count = 1  # em caso de erro, bloqueia por precaução

            if dw_count > 0:
                cur.close(); conn.close()
                raise HTTPException(
                    status_code=409,
                    detail=f"Este ficheiro tem {dw_count} linha(s) no DW. Não pode ser apagado diretamente."
                )

        cur.execute("DELETE FROM op_data WHERE file_id = %s", (file_id,))
        cur.execute("DELETE FROM etl_logs_dados WHERE file_id = %s", (str(file_id),))
        conn.commit()
        cur.close(); conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    try:
        s3 = get_s3()
        s3.delete_object(Bucket=BUCKET_RAW, Key=str(file_id))
    except Exception:
        pass

    return {"deleted": file_id}


@app.delete("/op_report/{report_id}")
def delete_op_report(report_id: int):
    """Apaga relatório (CASCADE em op_data) apenas se ainda não processado por etl_pdfs."""
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT pipeline_status, file_name FROM op_report WHERE report_id = %s", (report_id,))
        report = cur.fetchone()
        if not report:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Relatório não encontrado.")

        if report["pipeline_status"] not in ("PENDING", "FAILED"):
            cur.close(); conn.close()
            raise HTTPException(
                status_code=409,
                detail="Este relatório já foi processado pelo pipeline de PDFs e não pode ser apagado diretamente."
            )

        cur.execute("""
            SELECT COUNT(*) FROM op_data
            WHERE report_id = %s AND pipeline_status NOT IN ('PENDING', 'FAILED')
        """, (report_id,))
        if cur.fetchone()["count"] > 0:
            cur.close(); conn.close()
            raise HTTPException(
                status_code=409,
                detail="Este relatório tem dados já processados pelo pipeline de dados e não pode ser apagado diretamente."
            )

        file_name = report["file_name"]
        cur.execute("DELETE FROM op_report WHERE report_id = %s", (report_id,))
        conn.commit()
        cur.close(); conn.close()

        try:
            s3 = get_s3()
            s3.delete_object(Bucket=BUCKET_UNSTRUCTURED, Key=file_name)
        except Exception:
            pass

        try:
            conn_pipe = get_pipeline_connection()
            cur_pipe = conn_pipe.cursor()
            cur_pipe.execute("DELETE FROM etl_logs_pdfs WHERE report_id = %s", (report_id,))
            conn_pipe.commit()
            cur_pipe.close(); conn_pipe.close()
        except Exception:
            pass

        return {"deleted": report_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/extract_functions")
def get_extract_functions():
    excluded = {"clean_dataframe"}
    sf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "Pipeline", "silver_functions.py"))
    static_names = []
    try:
        with open(sf_path, encoding="utf-8") as f:
            for line in f:
                m = re.match(r'^def ([A-Za-z][A-Za-z0-9_]*)\s*\(', line)
                if m:
                    name = m.group(1)
                    if not name.startswith("_") and name not in excluded:
                        static_names.append(name)
    except Exception:
        pass
    auto = _load_auto_store()
    names = static_names + [n for n in auto if n not in static_names]
    return [{"name": n} for n in names]


# ════════════════════════════════════════════════════════════
# ENDPOINTS — Mapeamento source_code → extract_function
# ════════════════════════════════════════════════════════════

class FunctionMappingIn(BaseModel):
    source_code: str
    extract_function: str
    generation_hint: Optional[str] = None


class MappingPatchIn(BaseModel):
    extract_function: Optional[str] = None
    ai_extract_function: Optional[str] = None
    generation_hint: Optional[str] = None


@app.get("/function_mappings")
def get_function_mappings():
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT source_code, extract_function, ai_extract_function, generation_hint FROM source_function_mapping ORDER BY source_code")
        rows = cur.fetchall()
        cur.close(); conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/function_mappings", status_code=201)
def upsert_function_mapping(data: FunctionMappingIn):
    if not data.source_code.strip() or not data.extract_function.strip():
        raise HTTPException(status_code=400, detail="source_code e extract_function são obrigatórios.")
    hint = data.generation_hint.strip() if data.generation_hint and data.generation_hint.strip() else None
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO source_function_mapping (source_code, extract_function, generation_hint)
            VALUES (%s, %s, %s)
            ON CONFLICT (source_code) DO UPDATE
              SET extract_function = EXCLUDED.extract_function,
                  generation_hint  = COALESCE(EXCLUDED.generation_hint, source_function_mapping.generation_hint)
        """, (data.source_code.strip(), data.extract_function.strip(), hint))
        conn.commit()
        cur.close(); conn.close()
        return {"message": f"Mapeamento '{data.source_code}' → '{data.extract_function}' guardado."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/function_mappings/{source_code}")
def patch_mapping(source_code: str, data: MappingPatchIn):
    """Atualiza extract_function, ai_extract_function e/ou generation_hint."""
    fields = {}
    if data.extract_function is not None:
        fields["extract_function"] = data.extract_function.strip() or None
    if data.ai_extract_function is not None:
        fields["ai_extract_function"] = data.ai_extract_function.strip() or None
    if data.generation_hint is not None:
        fields["generation_hint"] = data.generation_hint.strip() or None
    if not fields:
        raise HTTPException(status_code=400, detail="Nenhum campo para atualizar.")
    set_clause = ", ".join(f"{k} = %s" for k in fields)
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute(
            f"UPDATE source_function_mapping SET {set_clause} WHERE source_code = %s",
            list(fields.values()) + [source_code],
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"Mapeamento para '{source_code}' não encontrado.")
        conn.commit()
        cur.close(); conn.close()
        return {"message": f"Mapeamento '{source_code}' atualizado."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/function_mappings/{source_code}/hint")
def update_hint(source_code: str, body: dict):
    """Atualiza apenas o generation_hint de um source_code."""
    hint = body.get("generation_hint", "")
    hint = hint.strip() if hint else None
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO source_function_mapping (source_code, generation_hint)
            VALUES (%s, %s)
            ON CONFLICT (source_code) DO UPDATE SET generation_hint = EXCLUDED.generation_hint
        """, (source_code, hint))
        conn.commit()
        cur.close(); conn.close()
        return {"message": f"Hint para '{source_code}' atualizado."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/function_mappings/{source_code}")
def delete_function_mapping(source_code: str):
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM source_function_mapping WHERE source_code = %s", (source_code,))
        if cur.rowcount == 0:
            conn.rollback(); cur.close(); conn.close()
            raise HTTPException(status_code=404, detail=f"Mapeamento para '{source_code}' não encontrado.")
        conn.commit()
        cur.close(); conn.close()
        return {"deleted": source_code}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/function_mappings/{source_code}/ai")
def delete_ai_function_mapping(source_code: str):
    """Limpa apenas o mapeamento AI para um source_code."""
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE source_function_mapping SET ai_extract_function = NULL WHERE source_code = %s",
            (source_code,)
        )
        if cur.rowcount == 0:
            conn.rollback(); cur.close(); conn.close()
            raise HTTPException(status_code=404, detail=f"Mapeamento para '{source_code}' não encontrado.")
        # Só apaga a linha se não houver nada útil (funções nem hint)
        cur.execute(
            "DELETE FROM source_function_mapping WHERE source_code = %s AND extract_function IS NULL AND ai_extract_function IS NULL AND generation_hint IS NULL",
            (source_code,)
        )
        conn.commit()
        cur.close(); conn.close()
        return {"cleared_ai": source_code}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/source_codes")
def get_source_codes():
    """Lista os source_codes distintos registados em op_report."""
    try:
        conn = get_operational_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT source_code FROM op_report WHERE source_code IS NOT NULL AND source_code <> '' ORDER BY source_code")
        rows = cur.fetchall()
        cur.close(); conn.close()
        return [r[0] for r in rows]
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

@app.get("/dashboard/reports")
def get_dashboard_reports():
    """Devolve apenas os relatórios que têm dados em fact_values."""
    try:
        conn_wh = get_warehouse_connection()
        conn_op = get_operational_connection()
        cur_wh = conn_wh.cursor()
        cur_op = conn_op.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur_wh.execute("SELECT DISTINCT report_id FROM fact_values WHERE report_id IS NOT NULL")
        report_ids = [r[0] for r in cur_wh.fetchall()]
        cur_wh.close()
        conn_wh.close()

        if not report_ids:
            cur_op.close()
            conn_op.close()
            return []

        cur_op.execute("""
            SELECT report_id, source_code, file_name, publication_date
            FROM op_report
            WHERE report_id = ANY(%s)
            ORDER BY report_id DESC;
        """, (report_ids,))
        rows = cur_op.fetchall()
        cur_op.close()
        conn_op.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

def _stream_script(script_path: str, python_exe: str | None = None):
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail=f"Script não encontrado: {script_path}")

    python_exe  = python_exe or sys.executable
    script_name = os.path.basename(script_path)
    script_dir  = os.path.dirname(script_path)

    def stream_output():
        yield f"A iniciar {script_name} ({os.path.basename(python_exe)})...\n"
        try:
            process = subprocess.Popen(
                [python_exe, script_path],
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


@app.post("/shutdown")
def shutdown():
    """Para os containers Docker e encerra o servidor."""
    def _stop():
        import time
        time.sleep(0.8)
        for c in ["projeto_uc", "projeto_pgadmin", "minio"]:
            subprocess.run(
                f"docker stop {c}", shell=True, capture_output=True,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        os._exit(0)
    threading.Thread(target=_stop, daemon=True).start()
    return {"message": "Sistema a encerrar..."}


@app.post("/etl/run")
def etl_run():
    return etl_run_dados()


@app.post("/etl/run/dados")
def etl_run_dados():
    global _dados_running, _dados_pending

    with _dados_state_lock:
        if _dados_running:
            _dados_pending = True
            def _already():
                yield "Pipeline de dados já está a correr — será reexecutada ao terminar.\n✓\n"
            return StreamingResponse(_already(), media_type="text/plain")
        _dados_running = True

    script_name = os.path.basename(PIPELINE_DADOS_SCRIPT)
    script_dir  = os.path.dirname(PIPELINE_DADOS_SCRIPT)

    def _stream():
        global _dados_running, _dados_pending
        yield f"A iniciar {script_name}...\n"
        try:
            proc = subprocess.Popen(
                [sys.executable, PIPELINE_DADOS_SCRIPT],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=script_dir,
            )
            for line in proc.stdout:
                yield line
            proc.wait()
            if proc.returncode == 0:
                yield f"\n✓ {script_name} concluído com sucesso.\n"
            else:
                yield f"\n✗ {script_name} terminou com erro (código {proc.returncode}).\n"
        except Exception as e:
            yield f"\n✗ Erro: {e}\n"
        finally:
            start_bg = False
            with _dados_state_lock:
                if _dados_pending:
                    _dados_pending = False
                    start_bg = True
                else:
                    _dados_running = False
            if start_bg:
                threading.Thread(target=_run_dados_loop, daemon=True).start()

    return StreamingResponse(_stream(), media_type="text/plain")


@app.get("/etl/status/dados")
def etl_status_dados():
    with _dados_state_lock:
        return {"running": _dados_running, "pending": _dados_pending}


@app.get("/pipeline/dados/log")
def pipeline_dados_log_endpoint():
    with _dados_log_lock:
        run_id = _dados_log_run_id
        lines  = list(_dados_log_lines)
    with _dados_state_lock:
        running = _dados_running
        pending = _dados_pending
    return {"run_id": run_id, "lines": lines, "running": running, "pending": pending}


@app.get("/pipeline/pdfs/log")
def pipeline_pdfs_log_endpoint():
    with _pdfs_log_lock:
        run_id = _pdfs_log_run_id
        lines  = list(_pdfs_log_lines)
    with _pdfs_state_lock:
        running = _pdfs_running
        pending = _pdfs_pending
    return {"run_id": run_id, "lines": lines, "running": running, "pending": pending}


@app.get("/etl/status/pdfs")
def etl_status_pdfs():
    step = "idle"
    try:
        with open(_PDFS_STEP_FILE, encoding="utf-8") as f:
            step = json.load(f).get("step", "idle")
    except Exception:
        pass
    with _pdfs_state_lock:
        return {"running": _pdfs_running, "pending": _pdfs_pending, "step": step}


@app.post("/etl/run/pdfs")
def etl_run_pdfs():
    return _stream_script(PIPELINE_PDFS_SCRIPT, python_exe=_PDFS_PYTHON)


@app.post("/etl/reset")
def etl_reset():
    return _stream_script(RESET_PIPELINE_SCRIPT)


@app.get("/etl_logs")
def get_etl_logs():
    try:
        conn = get_pipeline_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, file_id, NULL::integer AS report_id, file_name, step, error_message, log_time, 'dados' AS pipeline FROM etl_logs_dados
            UNION ALL
            SELECT id, NULL AS file_id, report_id, file_name, step, error_message, log_time, 'pdfs' AS pipeline FROM etl_logs_pdfs
            ORDER BY log_time DESC LIMIT 500
        """)
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
            SELECT report_id, file_name, step, error_message, log_time
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


@app.delete("/etl_logs/dados")
def clear_etl_logs_dados():
    """Apaga todos os registos de etl_logs_dados."""
    try:
        conn = get_pipeline_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM etl_logs_dados")
        deleted = cur.rowcount
        conn.commit()
        cur.close(); conn.close()
        return {"deleted": deleted}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/etl_logs/pdfs")
def clear_etl_logs_pdfs():
    """Apaga todos os registos de etl_logs_pdfs."""
    try:
        conn = get_pipeline_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM etl_logs_pdfs")
        deleted = cur.rowcount
        conn.commit()
        cur.close(); conn.close()
        return {"deleted": deleted}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/etl_logs/errors_since")
def get_etl_errors_since(tipo: str = "dados", minutes: int = 15):
    table = "etl_logs_dados" if tipo == "dados" else "etl_logs_pdfs"
    try:
        conn = get_pipeline_connection()
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM {table} WHERE log_time > NOW() - INTERVAL '{minutes} minutes'"
        )
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"errors": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ════════════════════════════════════════════════════════════
# ENDPOINTS — Configurações de pipeline
# ════════════════════════════════════════════════════════════

def _ensure_settings_table():
    conn = get_pipeline_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_settings (
                key   VARCHAR(100) PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.commit()
        cur.close()
    finally:
        conn.close()


@app.get("/settings/embedding-provider")
def get_embedding_provider():
    try:
        _ensure_settings_table()
        conn = get_pipeline_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT value FROM pipeline_settings WHERE key = 'embedding_provider'")
            row = cur.fetchone()
            cur.close()
            return {"provider": row[0] if row else "ollama"}
        finally:
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class EmbeddingProviderUpdate(BaseModel):
    provider: str


@app.patch("/settings/embedding-provider")
def set_embedding_provider(body: EmbeddingProviderUpdate):
    if body.provider not in ("ollama", "google"):
        raise HTTPException(status_code=400, detail="provider deve ser 'ollama' ou 'google'")
    try:
        _ensure_settings_table()
        conn = get_pipeline_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO pipeline_settings (key, value)
                VALUES ('embedding_provider', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (body.provider,))
            conn.commit()
            cur.close()
            return {"provider": body.provider}
        finally:
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _get_setting(key: str, default: str) -> str:
    try:
        _ensure_settings_table()
        conn = get_pipeline_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT value FROM pipeline_settings WHERE key = %s", (key,))
            row = cur.fetchone()
            cur.close()
            return row[0] if row else default
        finally:
            conn.close()
    except Exception:
        return default


def _set_setting(key: str, value: str) -> None:
    _ensure_settings_table()
    conn = get_pipeline_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_settings (key, value)
            VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (key, value))
        conn.commit()
        cur.close()
    finally:
        conn.close()


@app.get("/settings/llm-provider")
def get_llm_provider():
    try:
        return {"provider": _get_setting("llm_provider", "ollama")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class LlmProviderUpdate(BaseModel):
    provider: str


@app.patch("/settings/llm-provider")
def set_llm_provider(body: LlmProviderUpdate):
    if body.provider not in ("ollama", "google"):
        raise HTTPException(status_code=400, detail="provider deve ser 'ollama' ou 'google'")
    try:
        _set_setting("llm_provider", body.provider)
        return {"provider": body.provider}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/settings/function-provider")
def get_function_provider():
    try:
        return {"provider": _get_setting("function_provider", "ollama")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class FunctionProviderUpdate(BaseModel):
    provider: str


@app.patch("/settings/function-provider")
def set_function_provider(body: FunctionProviderUpdate):
    if body.provider not in ("ollama", "google"):
        raise HTTPException(status_code=400, detail="provider deve ser 'ollama' ou 'google'")
    try:
        _set_setting("function_provider", body.provider)
        return {"provider": body.provider}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/settings/google-api-key")
def get_google_api_key():
    try:
        key = _get_setting("google_api_key", "")
        return {"has_key": bool(key), "masked": ("●" * 20) if key else ""}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class GoogleApiKeyUpdate(BaseModel):
    key: str


@app.patch("/settings/google-api-key")
def set_google_api_key(body: GoogleApiKeyUpdate):
    try:
        _set_setting("google_api_key", body.key.strip())
        return {"has_key": bool(body.key.strip())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ════════════════════════════════════════════════════════════
# ENDPOINT — Chat
# ════════════════════════════════════════════════════════════

@app.get("/op_report/{report_id}/thumbnail")
def get_report_thumbnail(report_id: int):
    """Serve thumbnail JPEG da primeira página do PDF. Usa cache MinIO; gera na primeira chamada."""
    try:
        import fitz  # noqa: F401
    except ImportError:
        raise HTTPException(status_code=503, detail="PyMuPDF não instalado.")

    from fastapi.responses import Response

    s3 = get_s3()

    # 1. Servir do cache se já existir
    try:
        obj = s3.get_object(Bucket=BUCKET_THUMBNAILS, Key=f"{report_id}.jpg")
        return Response(
            content=obj["Body"].read(),
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"},
        )
    except Exception:
        pass

    # 2. Obter metadados do relatório
    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT file_name, report_url FROM op_report WHERE report_id = %s", (report_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(status_code=404, detail="Relatório não encontrado.")

    pdf_bytes = None

    # 3. Tentar MinIO
    try:
        obj = s3.get_object(Bucket=BUCKET_UNSTRUCTURED, Key=row["file_name"])
        pdf_bytes = obj["Body"].read()
    except Exception:
        pass

    # 4. Fallback: URL externo
    if pdf_bytes is None and row.get("report_url"):
        try:
            import requests as _req
            resp = _req.get(
                row["report_url"],
                timeout=30,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/pdf,*/*",
                },
                allow_redirects=True,
            )
            resp.raise_for_status()
            if resp.content[:4] == b"%PDF":
                pdf_bytes = resp.content
        except Exception:
            pass

    if not pdf_bytes or not pdf_bytes.startswith(b"%PDF"):
        raise HTTPException(status_code=404, detail="PDF não encontrado ou inválido.")

    img_bytes = _make_thumbnail(pdf_bytes)
    if not img_bytes:
        raise HTTPException(status_code=500, detail="Erro ao gerar thumbnail.")

    _cache_thumbnail(s3, report_id, img_bytes)

    return Response(
        content=img_bytes,
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.post("/op_report/thumbnails/rebuild")
def rebuild_thumbnails():
    """Regenera thumbnails em falta para todos os relatórios que têm PDF no MinIO."""
    try:
        import fitz  # noqa: F401
    except ImportError:
        raise HTTPException(status_code=503, detail="PyMuPDF não instalado.")

    s3 = get_s3()
    ensure_bucket(s3, BUCKET_THUMBNAILS)

    existing_thumbs: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_THUMBNAILS):
        for obj in page.get("Contents", []):
            existing_thumbs.add(obj["Key"])

    try:
        conn = get_operational_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT report_id, file_name FROM op_report ORDER BY report_id")
        reports = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    done, skipped, failed = 0, 0, 0
    for r in reports:
        thumb_key = f"{r['report_id']}.jpg"
        if thumb_key in existing_thumbs:
            skipped += 1
            continue
        pdf_bytes = None
        try:
            obj = s3.get_object(Bucket=BUCKET_UNSTRUCTURED, Key=r["file_name"])
            pdf_bytes = obj["Body"].read()
        except Exception:
            pass
        if not pdf_bytes:
            failed += 1
            continue
        img = _make_thumbnail(pdf_bytes)
        if img:
            _cache_thumbnail(s3, r["report_id"], img)
            done += 1
        else:
            failed += 1

    return {"generated": done, "skipped": skipped, "failed": failed}


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
