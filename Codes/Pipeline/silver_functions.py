import re as _re
import pandas as pd


def imf(data):
    indicators = data.get("indicators", {})
    values = data.get("values", {})

    rows = [
        {
            "location_code": location_code,
            "indicator_code": indicator_code,
            "indicator_name": indicators.get(indicator_code, {}).get("label"),
            "year": int(year),
            "value": float(value) if value is not None else None,
        }
        for indicator_code, countries_data in values.items()
        if countries_data is not None
        for location_code, years_data in countries_data.items()
        if years_data is not None
        for year, value in years_data.items()
    ]

    return pd.DataFrame(rows)


def hfi(data) -> pd.DataFrame:
    df = data if isinstance(data, pd.DataFrame) else pd.read_excel(data)
    df.columns = df.columns.str.lower().str.strip()

    if "iso" not in df.columns:
        # Caso 1: dois cabeçalhos — códigos de máquina na primeira linha de dados
        new_cols = [str(v).lower().strip() for v in df.iloc[0]]
        if "iso" in new_cols and "year" in new_cols:
            df.columns = new_cols
            df = df.iloc[1:].reset_index(drop=True)
        # Caso 2: primeiras colunas sem cabeçalho (unnamed: 0 = year, unnamed: 1 = iso)
        elif "human freedom" in df.columns:
            unnamed = [c for c in df.columns if c.startswith("unnamed:")]
            if len(unnamed) >= 2:
                df = df.rename(columns={unnamed[0]: "year", unnamed[1]: "iso"})
            if "human freedom" in df.columns:
                df = df.rename(columns={"human freedom": "hf_score"})

    missing = [c for c in ["iso", "year", "hf_score"] if c not in df.columns]
    if missing:
        raise ValueError(
            f"Colunas em falta: {missing} | "
            f"Colunas disponíveis ({len(df.columns)}): {list(df.columns[:15])}"
        )

    result = df[["iso", "year", "hf_score"]].copy()
    result = result.rename(columns={"iso": "location_code", "hf_score": "value"})
    result["indicator_code"] = "hf"
    result["indicator_name"] = "Human Freedom"
    result["year"]  = pd.to_numeric(result["year"],  errors="coerce").astype("Int64")
    result["value"] = pd.to_numeric(result["value"], errors="coerce")

    return (
        result[["location_code", "indicator_code", "indicator_name", "year", "value"]]
        .dropna(subset=["location_code", "year", "value"])
    )


def nri(data) -> pd.DataFrame:
    if not isinstance(data, pd.DataFrame):
        data = pd.read_excel(data)
    # Row 2 = nomes legíveis (Technology, Access, Mobile tariffs, ...)
    # Row 4 = códigos das colunas (NRI.score, 1.score, 1.1.1.score, ...)
    # Row 5+ = dados
    name_row = data.iloc[2].values
    code_row = data.iloc[4].values
    name_map = {
        str(code): str(name)
        for code, name in zip(code_row, name_row)
        if pd.notna(code) and pd.notna(name)
    }

    df = data.iloc[5:].copy()
    df.columns = [str(c) for c in code_row]
    df = df.reset_index(drop=True)

    meta = {"Economy", "ISO3Code", "region", "inc.group", "gdp.capita", "population", "ISO2code"}
    score_cols = [c for c in df.columns if "score" in c.lower() and c not in meta]

    rows = []
    for _, row in df.iterrows():
        loc = row.get("ISO3Code")
        if pd.isna(loc):
            continue
        for col in score_cols:
            val = row[col]
            if pd.isna(val):
                continue
            try:
                ind_code = col.replace(".score", "").replace("Score", "").strip(".")
                ind_name = name_map.get(col, col)
                rows.append({
                    "location_code": str(loc),
                    "indicator_code": ind_code,
                    "indicator_name": ind_name,
                    "year": 2025,
                    "value": float(val),
                })
            except (ValueError, TypeError):
                continue

    return pd.DataFrame(rows).dropna(subset=["location_code", "indicator_code", "year", "value"])


def heritage(data) -> pd.DataFrame:
    if not isinstance(data, pd.DataFrame):
        data = pd.read_excel(data)
    import pycountry as _pycountry

    def _to_iso3(name: str):
        try:
            return _pycountry.countries.search_fuzzy(name)[0].alpha_3
        except LookupError:
            return None

    # Row 0 = título "COMPONENT SCORES"; row 1 = headers reais; dados a partir de row 2
    # Quando lido com header=0 (default): data.iloc[0] = headers reais, data.iloc[1:] = dados
    df = data.iloc[1:].copy()
    df.columns = [str(c) for c in data.iloc[0].values]
    df = df.reset_index(drop=True)

    meta = {"Country", "Region"}
    value_cols = [c for c in df.columns if c not in meta and not str(c).startswith("Unnamed")]

    rows = []
    for _, row in df.iterrows():
        country = row.get("Country")
        if pd.isna(country) or str(country).strip() == "":
            continue
        iso3 = _to_iso3(str(country).strip())
        if iso3 is None:
            continue
        for col in value_cols:
            val = row[col]
            if pd.isna(val):
                continue
            try:
                rows.append({
                    "location_code": iso3,
                    "indicator_code": col,
                    "indicator_name": col,
                    "year": 2026,
                    "value": float(val),
                })
            except (ValueError, TypeError):
                continue

    return pd.DataFrame(rows).dropna(subset=["location_code", "indicator_code", "year", "value"])


def fraser(data) -> pd.DataFrame:
    if not isinstance(data, pd.DataFrame):
        data = pd.read_excel(data)
    # iloc[3] = headers reais; iloc[4:] = dados (4 linhas de metadados antes)
    df = data.iloc[4:].copy()
    df.columns = [str(c) for c in data.iloc[3].values]
    df = df.reset_index(drop=True)

    meta = {"nan", "Year", "ISO Code 2", "ISO Code 3", "Countries", "Rank", "Quartile",
            "World Bank Region", "World Bank Current Income Classification, 1990-Present"}
    value_cols = [c for c in df.columns
                  if c not in meta and c != "data" and "Rank" not in c and c.strip() != "nan"]

    rows = []
    for _, row in df.iterrows():
        iso  = row.get("ISO Code 3")
        year = row.get("Year")
        if pd.isna(iso) or pd.isna(year):
            continue
        for col in value_cols:
            val = row[col]
            if pd.isna(val):
                continue
            try:
                rows.append({
                    "location_code": str(iso).strip(),
                    "indicator_code": col.strip(),
                    "indicator_name": col.strip(),
                    "year": int(float(year)),
                    "value": float(val),
                })
            except (ValueError, TypeError):
                continue

    return pd.DataFrame(rows).dropna(subset=["location_code", "indicator_code", "year", "value"])


def wef_ttdi(data) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        df = data.reset_index(drop=True)
        # Detetar linha de cabeçalho dinamicamente (compatibilidade com testes manuais)
        attr_col = 9
        header_row = next(
            (i for i in range(min(10, len(df))) if str(df.iloc[i, attr_col]).strip() == "Attribute"),
            None,
        )
        if header_row is None:
            raise ValueError("Linha de cabeçalho com 'Attribute' não encontrada nas primeiras 10 linhas.")
    else:
        df = pd.read_excel(data, sheet_name="Dataset", header=None)
        header_row = 3  # estrutura fixa da sheet Dataset

    country_start = 10
    country_cols = list(range(country_start, df.shape[1]))
    countries = df.iloc[header_row, country_cols].tolist()

    data_df = df.iloc[header_row + 1:].copy()
    score_mask = data_df.iloc[:, 9].astype(str).str.strip().str.lower() == "score"
    score_df = data_df[score_mask]

    if score_df.empty:
        raise ValueError("Nenhuma linha com Attribute='Score' encontrada.")

    rows = []
    for _, row in score_df.iterrows():
        ind_code = str(row.iloc[6]).strip()
        ind_name = str(row.iloc[7]).strip()
        for i, iso3 in zip(country_cols, countries):
            val = row.iloc[i]
            if pd.isna(val):
                continue
            try:
                rows.append({
                    "location_code": str(iso3).strip(),
                    "indicator_code": ind_code,
                    "indicator_name": ind_name,
                    "year": 2024,
                    "value": float(val),
                })
            except (ValueError, TypeError):
                continue

    return pd.DataFrame(rows).dropna(subset=["location_code", "indicator_code", "year", "value"])


def epi(data) -> pd.DataFrame:
    df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)

    value_cols = [c for c in df.columns if _re.match(r'^.+\.raw\.\d{4}$', c, _re.IGNORECASE)]
    if not value_cols:
        raise ValueError(f"Nenhuma coluna INDICADOR.raw.ANO encontrada. Colunas: {list(df.columns[:10])}")

    indicator_code = value_cols[0].split(".")[0].upper()

    iso_col = next((c for c in df.columns if c.lower() == "iso"), None)
    if iso_col is None:
        raise ValueError("Coluna 'iso' não encontrada.")

    melted = df[[iso_col] + value_cols].melt(id_vars=iso_col, var_name="_col", value_name="value")
    melted["year"]           = melted["_col"].str.extract(r'(\d{4})$').astype(int)
    melted["value"]          = pd.to_numeric(melted["value"], errors="coerce")
    melted["location_code"]  = melted[iso_col]
    melted["indicator_code"] = indicator_code
    melted["indicator_name"] = indicator_code

    return (
        melted[["location_code", "indicator_code", "indicator_name", "year", "value"]]
        .dropna(subset=["location_code", "year", "value"])
        .reset_index(drop=True)
    )


# ══════════════════════════════════════════════════════════════
# REGISTO DE FUNÇÕES
# ══════════════════════════════════════════════════════════════
import inspect as _inspect, sys as _sys

_EXCLUDED = {"clean_dataframe"}

EXTRACT_FUNCTIONS = {
    name: obj
    for name, obj in _inspect.getmembers(_sys.modules[__name__], _inspect.isfunction)
    if not name.startswith("_")
    and name not in _EXCLUDED
    and obj.__module__ == __name__
}


# ══════════════════════════════════════════════════════════════
# AUTO-GENERATED FUNCTIONS
# ══════════════════════════════════════════════════════════════

def _load_auto_functions() -> dict:
    import os, json as _json
    store = os.path.join(os.path.dirname(__file__), "silver_functions_auto.json")
    if not os.path.exists(store):
        return {}
    try:
        with open(store, encoding="utf-8") as f:
            stored = _json.load(f)
    except Exception:
        return {}
    ns = {"pd": pd}
    loaded = {}
    for name, entry in stored.items():
        try:
            exec(compile(entry["code"], "<auto>", "exec"), ns)
            fn = ns.get(name)
            if callable(fn):
                loaded[name] = fn
        except Exception:
            pass
    return loaded


_auto = _load_auto_functions()
EXTRACT_FUNCTIONS.update(_auto)


# ══════════════════════════════════════════════════════════════
# LIMPEZA
# ══════════════════════════════════════════════════════════════

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    df = df.dropna(subset=["location_code", "indicator_code", "year", "value"])
    df["indicator_name"] = df["indicator_name"].fillna(df["indicator_code"])
    return df.reset_index(drop=True)
