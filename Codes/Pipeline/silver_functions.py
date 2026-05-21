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
    df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
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
    if "code" in df.columns:
        df = df.dropna(subset=["code"])
        if "name" in df.columns:
            df["name"] = df["name"].fillna(df["code"])
    if "location_code" in df.columns:
        df = df.dropna(subset=["location_code", "indicator_code", "year", "value"])
        if "indicator_name" in df.columns:
            df["indicator_name"] = df["indicator_name"].fillna(df["indicator_code"])
    return df.reset_index(drop=True)
