"""
Gerador automático de funções de transformação silver usando Ollama (qwen2.5:7b).

API pública:
    result = generate_and_validate(content: bytes, file_type: str) -> dict
    result = {
        "function_name": str,
        "code":          str | None,
        "fmt":           str,          # "json" | "csv" | "excel"
        "generated":     bool,
        "valid":         bool,
        "error":         str | None,
        "preview":       list | None,  # primeiras 3 linhas do DataFrame resultante
    }
"""

import io
import re
import json
import hashlib

import pandas as pd
import requests

OLLAMA_URL     = "http://localhost:11434/api/generate"
OLLAMA_MODEL   = "qwen2.5:7b"
OLLAMA_TIMEOUT = 300  # segundos

# ── System prompts especializados (exemplos inline, curtos) ───────────────────

_SYSTEM_COMBINED = """\
You are a data engineer. Generate a Python function that extracts data values.

Output DataFrame must have EXACTLY 5 columns:
  location_code (str), indicator_code (str), indicator_name (str), year (int), value (float)

Rules:
- Function argument is `data` (dict if JSON, DataFrame if CSV/Excel)
- pandas is available as `pd` — do NOT import anything
- Drop rows where location_code, indicator_code, year or value are None/NaN
- year must be int. In JSON all keys are strings — use int(year) directly, never isinstance checks
- If no indicator name is available, use indicator_code as indicator_name
- CRITICAL: any value in the JSON may be null at any nesting level. Always guard nested .items() calls:
    for ind, locs in data.get("values", {}).items() if isinstance(locs, dict)
    for loc, yrs in locs.items() if isinstance(yrs, dict)
- Return ONLY the function code, no markdown, no comments

JSON example:
def f(data):
    inds = data.get("indicators", {})
    rows = [{"location_code": loc, "indicator_code": ind, "indicator_name": inds.get(ind, {}).get("label", ind), "year": int(yr), "value": float(v) if v is not None else None} for ind, locs in data.get("values", {}).items() if isinstance(locs, dict) for loc, yrs in locs.items() if isinstance(yrs, dict) for yr, v in yrs.items()]
    return pd.DataFrame(rows).dropna(subset=["location_code","indicator_code","year","value"])

CSV/Excel example:
def f(data):
    skip = {"ISO_code", "countries", "region", "year", "rank"}
    ind_cols = [c for c in data.columns if c not in skip]
    rows = [{"location_code": r["ISO_code"], "indicator_code": c, "indicator_name": c, "year": int(r["year"]), "value": float(r[c])} for _, r in data.iterrows() for c in ind_cols if not pd.isna(r[c])]
    return pd.DataFrame(rows).dropna(subset=["location_code","indicator_code","year","value"])
"""

# ── Helpers internos ──────────────────────────────────────────────────────────

def _detect_format(content: bytes) -> str:
    snippet = content[:20].lstrip()
    if snippet.startswith(b"PK") or snippet.startswith(b"\xd0\xcf"):
        return "excel"
    if snippet.startswith(b"<?xml") or snippet.startswith(b"<"):
        return "xml"
    if snippet.startswith(b"{") or snippet.startswith(b"["):
        return "json"
    return "csv"


def _json_skeleton(obj, max_depth=3, max_items=4, _depth=0) -> str:
    """Representação compacta da estrutura de um objeto JSON.
    Mostra os primeiros N e os últimos 2 items para expor edge cases no fim."""
    indent = "  " * _depth
    if _depth >= max_depth:
        return f"{indent}..."
    if isinstance(obj, dict):
        all_items = list(obj.items())
        head = all_items[:max_items]
        tail = [x for x in all_items[-2:] if x not in head]  # últimos 2 se diferentes
        lines = [f"{indent}{{"]
        for k, v in head:
            val_str = _json_skeleton(v, max_depth, max_items, _depth + 1).strip()
            lines.append(f"{indent}  {json.dumps(k)}: {val_str}")
        if tail:
            lines.append(f"{indent}  ... ({len(obj) - max_items - len(tail)} more keys) ...")
            for k, v in tail:
                val_str = _json_skeleton(v, max_depth, max_items, _depth + 1).strip()
                lines.append(f"{indent}  {json.dumps(k)}: {val_str}")
        elif len(obj) > max_items:
            lines.append(f"{indent}  ... ({len(obj) - max_items} more keys)")
        lines.append(f"{indent}}}")
        return "\n".join(lines)
    if isinstance(obj, list):
        lines = [f"{indent}["]
        for item in obj[:max_items]:
            lines.append(_json_skeleton(item, max_depth, max_items, _depth + 1))
        if len(obj) > max_items:
            lines.append(f"{indent}  ... ({len(obj)} items total)")
        lines.append(f"{indent}]")
        return "\n".join(lines)
    return f"{indent}{json.dumps(obj)}"


_MAX_COLS_VALUE = 10   # colunas mostradas para tipo value
_MAX_ROWS_VALUE = 2    # linhas mostradas para tipo value
_MAX_CHARS      = 1500 # limite total de caracteres do sample


def _tabular_sample(df: pd.DataFrame, file_type: str) -> str:
    cols = list(df.columns)
    total = len(cols)
    if file_type == "indicator":
        # Só nomes de colunas — o modelo não precisa de ver dados
        shown = cols[:30]
        note  = f", ... ({total - 30} more)" if total > 30 else ""
        return f"Columns ({total} total): {shown}{note}"
    else:
        # value — mostra poucas colunas + poucas linhas
        shown_cols = cols[:_MAX_COLS_VALUE]
        note = f"\n  ... ({total - _MAX_COLS_VALUE} more columns)" if total > _MAX_COLS_VALUE else ""
        sample = f"Columns ({total} total): {shown_cols}{note}\n\n"
        sample += df[shown_cols].head(_MAX_ROWS_VALUE).to_string(index=False)
        return sample[:_MAX_CHARS]


def _build_sample(content: bytes, fmt: str, file_type: str = "value") -> tuple:
    """Devolve (sample_str, parsed_data) para passar ao Ollama e ao validador."""
    if fmt == "json":
        data = json.loads(content)
        sample = _json_skeleton(data, max_depth=3, max_items=5)
        return sample, data
    if fmt == "csv":
        df = pd.read_csv(io.BytesIO(content))
        return _tabular_sample(df, file_type), df
    if fmt == "excel":
        df = pd.read_excel(io.BytesIO(content))
        return _tabular_sample(df, file_type), df
    # fallback
    df = pd.read_csv(io.BytesIO(content))
    return _tabular_sample(df, file_type), df


def _extract_code(raw: str) -> str:
    """Remove blocos markdown ``` se o modelo os inserir."""
    match = re.search(r"```(?:python)?\s*\n?(.*?)```", raw, re.DOTALL)
    if match:
        return match.group(1).strip()
    return raw.strip()


def _patch_items_guards(code: str) -> str:
    """Adiciona isinstance(x, dict) guards em .items() aninhados dentro de comprehensions.

    Padrão problemático:
        for ind, locations in values.items()
        for loc, years in locations.items()   # locations pode ser None
    Após patch:
        for ind, locations in values.items() if isinstance(locations, dict)
        for loc, years in locations.items() if isinstance(years, dict)
    """
    # Encontra: "for VAR1, VAR2 in EXPR.items()" seguido de outro "for"
    # e adiciona "if isinstance(VAR2, dict)" se ainda não tiver
    pattern = re.compile(
        r'(for\s+\w+\s*,\s*(\w+)\s+in\s+[\w.()"\']+\.items\(\))'
        r'(?!\s*if\s+isinstance)'          # só se guard ainda não existir
        r'(\s*(?:\n\s*|\s+)for\s+\w)',     # seguido de outro for
    )

    def _add_guard(m: re.Match) -> str:
        var = m.group(2)
        return f"{m.group(1)} if isinstance({var}, dict){m.group(3)}"

    prev = None
    while prev != code:          # repete até não haver mais alterações (nested)
        prev = code
        code = pattern.sub(_add_guard, code)
    return code


def _make_function_name(fmt: str, file_type: str, content: bytes) -> str:
    h = hashlib.md5(content[:512]).hexdigest()[:4]
    tipo = "indicadores" if file_type == "indicator" else "valores"
    return f"{tipo}_{h}"


def _error_hint(error: str) -> str:
    """Devolve uma dica específica com base no tipo de erro de validação."""
    e = error.lower()
    if "nonetype" in e and "items" in e:
        return "Hint: some values in the dict are None — guard every .items() call with `if isinstance(x, dict)`."
    if "nonetype" in e:
        return "Hint: a variable is None when you try to use it — add None checks before accessing its attributes."
    if "empty" in e:
        return "Hint: the function returned an empty DataFrame — check that you are reading the correct keys from the data."
    if "missing" in e or "column" in e:
        return "Hint: the output is missing required columns — make sure all required columns are present in the returned DataFrame."
    if "syntax" in e:
        return "Hint: fix the Python syntax error in the function."
    if "int" in e or "float" in e or "valueerror" in e:
        return "Hint: a type conversion failed — add try/except or check for None before converting."
    return "Hint: rewrite the function more carefully following the rules."


def _call_ollama(system_prompt: str, user_prompt: str, temperature: float = 0.1) -> str:
    payload = {
        "model":  OLLAMA_MODEL,
        "system": system_prompt,
        "prompt": user_prompt,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": 512,
        },
    }
    resp = requests.post(OLLAMA_URL, json=payload, timeout=OLLAMA_TIMEOUT)
    resp.raise_for_status()
    return resp.json().get("response", "")


# ── Validador ─────────────────────────────────────────────────────────────────

_REQUIRED_COLS = {"location_code", "indicator_code", "indicator_name", "year", "value"}


def _validate(code: str, parsed_data, function_name: str) -> dict:
    namespace = {"pd": pd}
    try:
        exec(compile(code, "<generated>", "exec"), namespace)
    except SyntaxError as e:
        return {"valid": False, "error": f"Erro de sintaxe: {e}", "preview": None}

    fn = namespace.get(function_name)
    if fn is None or not callable(fn):
        return {"valid": False, "error": "Nenhuma função encontrada no código gerado.", "preview": None}

    try:
        df = fn(parsed_data)
    except Exception as e:
        return {"valid": False, "error": f"Erro ao executar a função: {e}", "preview": None}

    if not isinstance(df, pd.DataFrame):
        return {"valid": False, "error": "A função não devolveu um DataFrame.", "preview": None}
    if df.empty:
        return {"valid": False, "error": "A função devolveu um DataFrame vazio.", "preview": None}

    missing = _REQUIRED_COLS - set(df.columns)
    if missing:
        return {"valid": False, "error": f"Colunas em falta no output: {sorted(missing)}", "preview": None}

    preview = df.head(3).to_dict(orient="records")
    return {"valid": True, "error": None, "preview": preview}


# ── API pública ───────────────────────────────────────────────────────────────

def generate_and_validate(content: bytes) -> dict:
    """
    Gera e valida automaticamente uma função de transformação silver.

    Args:
        content: bytes do ficheiro de dados

    Returns:
        dict com as chaves:
          function_name, code, fmt, generated, valid, error, preview
    """
    fmt           = _detect_format(content)
    function_name = _make_function_name(fmt, "value", content)

    try:
        sample_str, parsed_data = _build_sample(content, fmt, "value")
    except Exception as e:
        return {
            "function_name": function_name, "code": None, "fmt": fmt,
            "generated": False, "valid": False,
            "error": f"Erro ao ler o ficheiro: {e}",
            "preview": None,
        }

    def _base_user_prompt():
        return (
            f"Generate a transformation function for the following {fmt.upper()} file.\n\n"
            f"FILE SAMPLE:\n{sample_str}\n\n"
            f"FUNCTION NAME: {function_name}\n\n"
            f"Return ONLY the Python function code. No markdown, no explanations."
        )

    code = None
    last_error = None

    for attempt in range(1, 4):
        if attempt == 1:
            user_prompt = _base_user_prompt()
            temperature = 0.1
        else:
            hint = _error_hint(last_error)
            user_prompt = (
                f"{_base_user_prompt()}\n\n"
                f"IMPORTANT — your previous attempt failed:\n"
                f"  Error: {last_error}\n"
                f"  {hint}\n"
                f"Rewrite the function from scratch fixing this issue."
            )
            temperature = 0.3

        try:
            raw  = _call_ollama(_SYSTEM_COMBINED, user_prompt, temperature=temperature)
            code = _patch_items_guards(_extract_code(raw))
        except Exception as e:
            return {
                "function_name": function_name, "code": code, "fmt": fmt,
                "generated": attempt > 1, "valid": False,
                "error": f"Ollama não respondeu (tentativa {attempt}): {e}",
                "preview": None,
            }

        validation = _validate(code, parsed_data, function_name)
        if validation["valid"]:
            break
        last_error = validation["error"]

    return {
        "function_name": function_name,
        "code":          code,
        "fmt":           fmt,
        "generated":     True,
        "valid":         validation["valid"],
        "error":         validation["error"],
        "preview":       validation["preview"],
    }


# ── Teste rápido via linha de comandos ────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso: python silver_function_generator.py <ficheiro>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path, "rb") as f:
        data = f.read()

    print(f"A gerar função para '{path}'...")
    result = generate_and_validate(data)

    print(f"\nFORMATO DETETADO : {result['fmt']}")
    print(f"NOME DA FUNÇÃO   : {result['function_name']}")
    print(f"GERADA           : {result['generated']}")
    print(f"VÁLIDA           : {result['valid']}")
    if result["error"]:
        print(f"ERRO             : {result['error']}")
    if result["preview"]:
        print(f"\nPREVIEW (3 linhas):")
        for row in result["preview"]:
            print(" ", row)
    if result["code"]:
        print(f"\n{'='*60}\nCÓDIGO GERADO:\n{'='*60}")
        print(result["code"])
