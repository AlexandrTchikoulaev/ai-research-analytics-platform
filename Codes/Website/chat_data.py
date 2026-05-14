"""
SQL Chatbot — Data Warehouse (tabular data).

Routing tiers:
  meta:<sub>          → Python puro (SQL fixo, 0 LLM)
  simple              → Ollama + template + fuzzy hint (1 país, 1 indicador, 1 ano)
  simple_inferred     → Ollama + template, ano = MAX(year) da BD
  existence           → Ollama + template de contagem
  existence_inferred  → idem, ano inferido
  complex / uncertain → Ollama + prompt de schema completo
"""

import re
import random
import warnings

import psycopg2
import psycopg2.pool
from langchain_community.llms.ollama import Ollama
from langchain_core.prompts import PromptTemplate
from rapidfuzz import process, fuzz

warnings.filterwarnings("ignore")

# ── Config ─────────────────────────────────────────────────────────────────
_DB_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "warehouse_db",
    "user":     "projeto_utilizador",
    "password": "projeto",
}

_OLLAMA_MODEL    = "qwen2.5:7b"
_FUZZY_THRESHOLD = 72

_ollama: Ollama | None = None

def _get_ollama() -> Ollama:
    global _ollama
    if _ollama is None:
        _ollama = Ollama(model=_OLLAMA_MODEL, temperature=0)
    return _ollama


# ── Connection pool ─────────────────────────────────────────────────────────
_pool: psycopg2.pool.ThreadedConnectionPool | None = None

def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=5, **_DB_CONFIG)
    return _pool


# ── Boot cache (loaded once on first request) ───────────────────────────────
_cache: dict = {
    "max_year":          None,
    "indicators":        {},   # {indicator_code: indicator_name}
    "indicator_list_str": "",
    "loaded":            False,
}

def _ensure_loaded() -> None:
    if _cache["loaded"]:
        return
    conn = _get_pool().getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(year) FROM dim_date")
            _cache["max_year"] = cur.fetchone()[0]

            cur.execute(
                "SELECT DISTINCT ON (indicator_code) indicator_code, indicator_name "
                "FROM dim_indicator ORDER BY indicator_code"
            )
            rows = cur.fetchall()
            _cache["indicators"]         = {code: name for code, name in rows}
            _cache["indicator_list_str"] = "\n".join(
                f"  {code} = {name}" for code, name in rows
            )
            _cache["loaded"] = True
    finally:
        _get_pool().putconn(conn)


# ── Fuzzy indicator matching ────────────────────────────────────────────────
def _fuzzy_indicator(text: str) -> tuple[str | None, str | None]:
    """Returns (indicator_code, indicator_name) or (None, None)."""
    inds = _cache["indicators"]
    if not inds:
        return None, None

    names  = list(inds.values())
    codes  = list(inds.keys())

    r = process.extractOne(text, names, scorer=fuzz.partial_ratio)
    if r and r[1] >= _FUZZY_THRESHOLD:
        code = codes[names.index(r[0])]
        return code, r[0]

    r = process.extractOne(text.upper(), codes, scorer=fuzz.partial_ratio)
    if r and r[1] >= _FUZZY_THRESHOLD:
        return r[0], inds[r[0]]

    return None, None


def _indicator_hint(question: str) -> str:
    _, name = _fuzzy_indicator(question)
    if name:
        return (
            f"Best match for indicator: \"{name}\". "
            "Use this name (or close variant) in the ILIKE filter unless clearly wrong."
        )
    lst = _cache["indicator_list_str"] or "(no indicators loaded)"
    return (
        f"Available indicators (use indicator_name column with ILIKE):\n{lst}\n"
        "Choose the most appropriate indicator_name."
    )


# ── Country PT → EN ─────────────────────────────────────────────────────────
_PT_EN: dict[str, str] = {
    # Ibérica
    "portugal": "Portugal",         "espanha": "Spain",
    # Europa Ocidental
    "alemanha": "Germany",          "frança": "France",          "franca": "France",
    "itália": "Italy",              "italia": "Italy",
    "reino unido": "United Kingdom",
    "estados unidos": "United States",
    "holanda": "Netherlands",       "países baixos": "Netherlands",
    "bélgica": "Belgium",           "belgica": "Belgium",
    "suécia": "Sweden",             "suecia": "Sweden",
    "noruega": "Norway",            "dinamarca": "Denmark",
    "finlândia": "Finland",         "finlandia": "Finland",
    "áustria": "Austria",           "austria": "Austria",
    "suíça": "Switzerland",         "suica": "Switzerland",
    "irlanda": "Ireland",           "islândia": "Iceland",       "islandia": "Iceland",
    "luxemburgo": "Luxembourg",     "malta": "Malta",            "chipre": "Cyprus",
    # Europa Central/Leste
    "polónia": "Poland",            "polonia": "Poland",
    "hungria": "Hungary",
    "república checa": "Czech Republic", "chéquia": "Czech Republic", "chequia": "Czech Republic",
    "eslováquia": "Slovakia",       "eslovaquia": "Slovakia",
    "eslovénia": "Slovenia",        "eslovenia": "Slovenia",
    "croácia": "Croatia",           "croacia": "Croatia",
    "bulgária": "Bulgaria",         "bulgaria": "Bulgaria",
    "roménia": "Romania",           "romania": "Romania",
    "estónia": "Estonia",           "estonia": "Estonia",
    "letónia": "Latvia",            "letonia": "Latvia",
    "lituânia": "Lithuania",        "lituania": "Lithuania",
    "grécia": "Greece",             "grecia": "Greece",
    # Balcãs
    "albânia": "Albania",           "albania": "Albania",
    "sérvia": "Serbia",             "serbia": "Serbia",
    "bósnia": "Bosnia and Herzegovina", "bosnia": "Bosnia and Herzegovina",
    "macedónia": "North Macedonia", "macedonia": "North Macedonia",
    "montenegro": "Montenegro",     "kosovo": "Kosovo",
    # Ex-URSS
    "rússia": "Russia",             "russia": "Russia",
    "ucrânia": "Ukraine",           "ucrania": "Ukraine",
    "bielorrússia": "Belarus",      "bielorrussia": "Belarus",
    "moldova": "Moldova",           "moldávia": "Moldova",       "moldavia": "Moldova",
    "arménia": "Armenia",           "armenia": "Armenia",
    "geórgia": "Georgia",           "georgia": "Georgia",
    "azerbaijão": "Azerbaijan",     "azerbaijao": "Azerbaijan",
    "cazaquistão": "Kazakhstan",    "cazaquistao": "Kazakhstan",
    # Médio Oriente / África / Ásia
    "turquia": "Turkey",
    "índia": "India",               "india": "India",
    "china": "China",               "japão": "Japan",            "japao": "Japan",
    "coreia do sul": "Korea, Republic of",
    "argélia": "Algeria",           "algeria": "Algeria",
    "egipto": "Egypt",              "egito": "Egypt",
    "marrocos": "Morocco",          "moçambique": "Mozambique",  "mocambique": "Mozambique",
    "áfrica do sul": "South Africa","africa do sul": "South Africa",
    "nigéria": "Nigeria",           "nigeria": "Nigeria",
    "etiópia": "Ethiopia",          "etiopia": "Ethiopia",
    # Américas
    "brasil": "Brazil",             "mexico": "Mexico",          "méxico": "Mexico",
    "argentina": "Argentina",       "canadá": "Canada",          "canada": "Canada",
    "colombia": "Colombia",         "colômbia": "Colombia",
    "chile": "Chile",               "peru": "Peru",              "venezuela": "Venezuela",
    # Oceania
    "austrália": "Australia",       "australia": "Australia",
    "nova zelândia": "New Zealand", "nova zelandia": "New Zealand",
}

# Palavras PT que começam com maiúscula mas não são países
_PT_STOP = {
    "Qual", "Quais", "Para", "Como", "Quando", "Onde", "Quanto", "Quem",
    "Por", "Em", "De", "Da", "Do", "No", "Na", "Com", "Sem", "Sobre",
    "Entre", "Após", "Antes", "Este", "Esta", "Esse", "Essa",
    "Aquele", "Aquela", "Valor", "Dados", "Indicador", "Ano", "Anos",
    "País", "Relatório", "Qual", "Mesmo", "Mesma",
}

def _extract_country(question: str) -> str | None:
    p = question.lower()
    for pt in sorted(_PT_EN, key=len, reverse=True):
        if pt in p:
            return _PT_EN[pt]
    # Heuristic: first capitalised word that isn't a PT stop word
    caps = re.findall(r'(?<!\. )\b[A-Z][a-záéíóúâêîôûãõàèìòùç]{2,}\b', question)
    for cap in caps:
        if cap not in _PT_STOP:
            return cap
    return None


# ── Classifier ──────────────────────────────────────────────────────────────
_COMPLEX_SIGNALS = [
    r"\be\s+\w{3,}", r"\bou\s+\w{3,}", r"\bvs\.?\b", r"\bversus\b", r"\bcompar",
    r"\bmedia\b", r"\bmédia\b", r"\bmédio\b", r"\bsoma\b",
    r"\bmáximo\b", r"\bmaximo\b", r"\bmínimo\b", r"\bminimo\b",
    r"\bcrescimento\b", r"\bdiferença\b", r"\bdiferenca\b", r"\bvariação\b",
    r"\bde \d{4} a \d{4}\b", r"\bentre \d{4} e \d{4}\b", r"\banos \d{4}",
    r"\btop\s*\d+\b", r"\bmelhores\b", r"\bpiores\b", r"\bpior\b",
    r"\bverifica\b", r"\bé maior\b", r"\bé menor\b", r"\bsuperior\b", r"\binferior\b",
    r"\bacima de\b", r"\babaixo de\b",
    r"\bevolução\b", r"\bevolucao\b", r"\bhistórico\b", r"\bhistorico\b",
    r"\bao longo\b", r"\btendência\b",
    r"\w+,\s*\w+\s+e\s+\w+",
    r"\búltimos\s*\d+\b", r"\batual\b", r"\bactual\b", r"\bmais recente\b",
    r"\bem que ano\b", r"\bque ano\b",
    r"\bpara quais\b", r"\bquais os\b", r"\btodos os\b",
]

_META_SIGNALS: list[tuple[str, str]] = [
    (r"\bquantos (registos|registros|dados|valores)\b", "count_records"),
    (r"\bquantos países\b",                             "count_countries"),
    (r"\bquantos indicadores\b",                        "count_indicators"),
    (r"\bquantos relatórios\b",                         "count_reports"),
    (r"\b(que|quais( os?)?) países\b",                  "list_countries"),
    (r"\b(lista[r]? |que |quais( os?)? )indicadores\b", "list_indicators"),
    (r"\bdados disponíveis\b",                          "count_records"),
]

_EXISTENCE_SIGNALS = [
    r"\btem dados\b", r"\bexiste[m]?\b", r"\bhá dados\b",
    r"\bcontém\b",    r"\bcontem\b",
]

_YEAR_RE       = re.compile(r"\b(19|20)\d{2}\b")
_MULTI_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b.*\b(19|20)\d{2}\b")


def _classify(question: str) -> tuple[str, str]:
    p = question.lower()

    for pattern, sub in _META_SIGNALS:
        if re.search(pattern, p):
            return f"meta:{sub}", f"meta: {pattern}"

    for pattern in _EXISTENCE_SIGNALS:
        if re.search(pattern, p):
            anos = _YEAR_RE.findall(question)
            return ("existence", f"existence, year={anos[0]}") if len(anos) == 1 \
                else ("existence_inferred", "existence, year inferred")

    for pattern in _COMPLEX_SIGNALS:
        if re.search(pattern, p):
            return "complex", f"complex: {pattern}"

    if _MULTI_YEAR_RE.search(question):
        return "complex", "multiple years"

    anos = _YEAR_RE.findall(question)
    if len(anos) == 1:
        return "simple", f"simple, year={anos[0]}"
    if len(anos) == 0:
        return "simple_inferred", "no year — use MAX"

    return "uncertain", "uncertain — fallback"


# ── Meta SQL (pure Python, 0 LLM) ───────────────────────────────────────────
_META_SQL: dict[str, str | None] = {
    "count_records":    "SELECT COUNT(*) AS total_registos FROM fact_values",
    "count_countries":  "SELECT COUNT(DISTINCT name) AS total_paises FROM dim_location",
    "count_indicators": "SELECT COUNT(DISTINCT indicator_code) AS total_indicadores FROM dim_indicator",
    "count_reports":    "SELECT COUNT(DISTINCT report_id) AS total_relatorios FROM fact_values",
    "list_countries":   "SELECT name AS pais FROM dim_location ORDER BY name",
    "list_indicators":  "SELECT DISTINCT indicator_code AS codigo, indicator_name AS nome "
                        "FROM dim_indicator ORDER BY indicator_code",
}


# ── Ollama prompt templates ─────────────────────────────────────────────────
_T_VALUE = """\
Output ONLY a valid SQL SELECT query. No explanation, no markdown, no Chinese, no other language.
{indicator_hint}

The view vw_indicator_location_year has columns: indicator_name, location_name, value, year.

Template:
SELECT year, value
FROM vw_indicator_location_year
WHERE indicator_name ILIKE '%[INDICATOR_NAME]%'
  AND location_name  ILIKE '%[COUNTRY_IN_ENGLISH]%'
  AND year = [YEAR];

Rules:
- Replace [INDICATOR_NAME] with the indicator name from the hint above.
- Translate country to English for [COUNTRY_IN_ENGLISH].
- Replace [YEAR] with the 4-digit year from the question.
Question: {question}
SQL:"""

_T_EXISTENCE = """\
Output ONLY a valid SQL SELECT query. No explanation, no markdown, no Chinese, no other language.
{indicator_hint}

The view vw_indicator_location_year has columns: indicator_name, location_name, value, year.

Template:
SELECT COUNT(*) AS registos_encontrados
FROM vw_indicator_location_year
WHERE indicator_name ILIKE '%[INDICATOR_NAME]%'
  AND location_name  ILIKE '%[COUNTRY_IN_ENGLISH]%'
  AND year = [YEAR];

Rules:
- Replace [INDICATOR_NAME] with the indicator name from the hint.
- Translate country to English for [COUNTRY_IN_ENGLISH].
- Replace [YEAR] with the 4-digit year.
Question: {question}
SQL:"""

_T_RANKING = """\
Output ONLY a valid SQL SELECT query. No explanation, no markdown, no Chinese, no other language.
{indicator_hint}

Tables available: fact_values(location_sk, indicator_sk, date_id, value),
dim_location(location_sk, name), dim_indicator(indicator_sk, indicator_name), dim_date(date_id, year).

Template:
SELECT year, ranking, value FROM (
    SELECT dl.name AS country, dd.year, fv.value,
           RANK() OVER (PARTITION BY dd.year ORDER BY fv.value DESC) AS ranking
    FROM fact_values fv
    JOIN dim_location  dl ON fv.location_sk  = dl.location_sk
    JOIN dim_date      dd ON fv.date_id      = dd.date_id
    JOIN dim_indicator di ON fv.indicator_sk = di.indicator_sk
    WHERE di.indicator_name ILIKE '%[INDICATOR_NAME]%'
) sub
WHERE country ILIKE '%[COUNTRY_IN_ENGLISH]%' AND year = [YEAR]
ORDER BY ranking ASC LIMIT 1;

Rules:
- Replace [INDICATOR_NAME] with indicator name from hint.
- Translate country to English for [COUNTRY_IN_ENGLISH].
- Replace [YEAR] with the 4-digit year.
Question: {question}
SQL:"""

_T_COMPLEX = """\
Output ONLY a valid PostgreSQL SELECT query. No explanation, no markdown, no Chinese, no other language.

Schema:
  View vw_indicator_location_year(indicator_name, location_name, value, year) — use for simple filters
  fact_values(report_id, location_sk, indicator_sk, date_id, value, value_type)
  dim_location(location_sk, location_code, name, region, sub_region)
  dim_indicator(indicator_sk, source_system, indicator_code, indicator_name)
  dim_date(date_id, year)
  Joins: fact_values → dim_location via location_sk, → dim_indicator via indicator_sk, → dim_date via date_id

Rules:
- Translate Portuguese country names to English (e.g. Alemanha→Germany, Espanha→Spain). Use ILIKE '%%name%%'.
- Rankings: RANK() OVER (PARTITION BY dd.year ORDER BY fv.value DESC).
- Date ranges: dd.year BETWEEN x AND y.
- Never expose internal _sk or _id columns in results.
{indicator_hint}

Question: {question}
SQL:"""

_RANKING_WORDS = ["lugar", "posição", "posicao", "ranking", "rank", "classificação", "classificacao"]


def _pick_template(tier: str, question: str) -> str:
    if tier in ("existence", "existence_inferred"):
        return _T_EXISTENCE
    if tier in ("complex", "uncertain"):
        return _T_COMPLEX
    if any(w in question.lower() for w in _RANKING_WORDS):
        return _T_RANKING
    return _T_VALUE


# ── SQL extraction & validation ─────────────────────────────────────────────
def _extract_sql(text: str) -> str:
    m = re.search(r"```(?:sql)?\n?(.*?)\n?```", text, re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else text.strip()


def _validate_sql(sql: str) -> None:
    if not sql.strip().upper().startswith("SELECT"):
        raise ValueError(f"Não é um SELECT: {sql[:80]}")
    if re.search(r"\[.+?\]", sql):
        raise ValueError(f"Placeholders por preencher: {sql[:80]}")

def _explain_sql(sql: str) -> None:
    """EXPLAIN catches alias/join errors (e.g. 'd.year' without a dim_date join) before executing."""
    conn = _get_pool().getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN {sql}")
    finally:
        _get_pool().putconn(conn)


# ── Fallback: build SQL purely from extracted entities ──────────────────────
def _fallback_sql(question: str, tier: str) -> str:
    anos = _YEAR_RE.findall(question)
    year = anos[0] if anos else (str(_cache["max_year"]) if _cache["max_year"] else None)
    if not year:
        raise RuntimeError("Não foi possível determinar o ano.")

    _, ind_name = _fuzzy_indicator(question)
    if not ind_name:
        raise RuntimeError("Não foi possível identificar o indicador.")

    country = _extract_country(question)
    if not country:
        raise RuntimeError("Não foi possível identificar o país.")

    base_where = (
        f"WHERE indicator_name ILIKE '%{ind_name}%' "
        f"AND location_name ILIKE '%{country}%' "
        f"AND year = {year}"
    )

    if tier in ("existence", "existence_inferred"):
        return f"SELECT COUNT(*) AS registos_encontrados FROM vw_indicator_location_year {base_where}"

    if any(w in question.lower() for w in _RANKING_WORDS):
        return (
            f"SELECT year, ranking, value FROM ("
            f"SELECT dl.name AS country, dd.year, fv.value, "
            f"RANK() OVER (PARTITION BY dd.year ORDER BY fv.value DESC) AS ranking "
            f"FROM fact_values fv "
            f"JOIN dim_location dl ON fv.location_sk = dl.location_sk "
            f"JOIN dim_date dd ON fv.date_id = dd.date_id "
            f"JOIN dim_indicator di ON fv.indicator_sk = di.indicator_sk "
            f"WHERE di.indicator_name ILIKE '%{ind_name}%'"
            f") sub WHERE country ILIKE '%{country}%' AND year = {year} "
            f"ORDER BY ranking ASC LIMIT 1"
        )

    return f"SELECT year, value FROM vw_indicator_location_year {base_where}"


# ── Generate SQL via Ollama ─────────────────────────────────────────────────
def _gen_sql(question: str, tier: str) -> str:
    if tier in ("simple_inferred", "existence_inferred") and _cache["max_year"]:
        q = f"{question} (year to use: {_cache['max_year']})"
    else:
        q = question

    tmpl     = _pick_template(tier, question)
    hint     = _indicator_hint(question)
    prompt   = PromptTemplate.from_template(tmpl)
    response = _get_ollama().invoke(prompt.format(question=q, indicator_hint=hint))
    sql      = _extract_sql(response)

    try:
        _validate_sql(sql)
        _explain_sql(sql)   # catches alias/join errors the model may introduce
        return sql
    except Exception:
        return _fallback_sql(question, tier)


# ── Execute query ───────────────────────────────────────────────────────────
def _execute(sql: str) -> tuple[list, list]:
    conn = _get_pool().getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        return cols, rows
    finally:
        _get_pool().putconn(conn)


# ── Format results as plain-text table ─────────────────────────────────────
def _format_table(cols: list, rows: list) -> str:
    if not rows:
        return "Nenhum resultado encontrado."
    rows = [tuple("" if v is None else v for v in r) for r in rows]
    widths = [
        max(len(str(c)), max((len(str(r[i])) for r in rows), default=0))
        for i, c in enumerate(cols)
    ]
    def fmt(r: tuple) -> str:
        return "  ".join(str(v).ljust(w) for v, w in zip(r, widths))
    sep = "  ".join("─" * w for w in widths)
    return "\n".join([fmt(cols), sep] + [fmt(r) for r in rows])


# ── Natural language response (0 LLM) ──────────────────────────────────────
_FRASES_VALOR = [
    "Em {year}, {country} registou um valor de {value} para {indicator}.",
    "{country} apresentou {value} em {indicator} no ano de {year}.",
    "O valor de {indicator} para {country} em {year} foi de {value}.",
]
_FRASES_RANKING = [
    "{country} ficou na {ranking}.ª posição no ranking de {indicator} em {year}, com {value}.",
    "Em {year}, {country} ocupou o {ranking}.º lugar em {indicator} (valor: {value}).",
]
_FRASES_EXIST = [
    "Foram encontrados {count} registo(s) de {indicator} para {country} em {year}.",
    "A base de dados contém {count} entrada(s) de {indicator} para {country} no ano {year}.",
]
_FRASES_NADA = [
    "Não foram encontrados dados para esta consulta.",
    "Sem resultados para os parâmetros indicados.",
]


def _naturalize(question: str, tier: str, cols: list, rows: list) -> str:
    if not rows:
        return random.choice(_FRASES_NADA)
    if tier.startswith("meta:"):
        return ""

    row  = rows[0]
    data = {c: v for c, v in zip(cols, row)}
    _, ind_name = _fuzzy_indicator(question)
    indicator   = ind_name or data.get("indicator_name", "indicador")
    country     = _extract_country(question) or data.get("location_name", "")
    year        = data.get("year", "")
    value       = data.get("value", "")

    if tier in ("existence", "existence_inferred"):
        return random.choice(_FRASES_EXIST).format(
            count=row[0], indicator=indicator, country=country, year=year
        )
    if "ranking" in [c.lower() for c in cols]:
        return random.choice(_FRASES_RANKING).format(
            country=country, ranking=data.get("ranking", ""),
            indicator=indicator, year=year, value=value,
        )
    return random.choice(_FRASES_VALOR).format(
        year=year, country=country, value=value, indicator=indicator
    )


# ── Public entry point ──────────────────────────────────────────────────────
def chatbot_sql(question: str) -> str:
    _ensure_loaded()
    tier, _ = _classify(question)

    if tier.startswith("meta:"):
        sub = tier.split(":")[1]
        sql = _META_SQL.get(sub)
        if sql is None:
            tier = "complex"
        else:
            try:
                cols, rows = _execute(sql)
                phrase = _naturalize(question, tier, cols, rows)
                table  = _format_table(cols, rows)
                parts  = [p for p in [phrase, table] if p]
                return "\n\n".join(parts)
            except Exception as e:
                return f"Erro ao executar query: {e}"

    try:
        sql = _gen_sql(question, tier)
    except RuntimeError as e:
        return str(e)

    try:
        cols, rows = _execute(sql)
    except Exception as e:
        return f"Erro ao executar query: {e}"

    phrase = _naturalize(question, tier, cols, rows)
    table  = _format_table(cols, rows)
    parts  = [p for p in [phrase, table] if p]
    return "\n\n".join(parts)
