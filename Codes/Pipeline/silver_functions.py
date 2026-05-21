import pandas as pd

# ------------------------------------------------------------------
#       FUNÇÕES GERAIS
# ------------------------------------------------------------------


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

import pandas as pd

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

    result = result.rename(columns={
        "iso": "location_code",
        "year": "year",
        "hf_score": "value"
    })

    result["indicator_code"] = "hf"
    result["indicator_name"] = "Human Freedom"

    result["year"]  = pd.to_numeric(result["year"],  errors="coerce").astype("Int64")
    result["value"] = pd.to_numeric(result["value"], errors="coerce")

    result = result[[
        "location_code",
        "indicator_code",
        "indicator_name",
        "year",
        "value"
    ]]

    return result.dropna(subset=["location_code", "year", "value"])


# ------------------------------------------------------------------
# ------------------------------------------------------------------

def imf_indicadores(data):
    items = data.get("indicators", {})
    return pd.DataFrame({
        "code": list(items.keys()),
        "name": [v.get("label") for v in items.values()],
    })


import pandas as pd

def imf_values(data):
    values = data.get("values", {})

    rows = [
        {
            "location_code": loc,
            "indicator_code": ind,
            "year": int(year),
            "value": float(val) if val is not None else None,
        }
        for ind, locations in values.items()
        if locations is not None
        for loc, years in locations.items()
        if years is not None
        for year, val in years.items()
    ]

    return pd.DataFrame(rows)


def hfi_indicadores(_data=None):
    return pd.DataFrame({
        "code": ["HF"],
        "name": ["human freedom index"],
    })


def hfi_values(df: pd.DataFrame) -> pd.DataFrame:

    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    # Construção do novo dataframe
    out = pd.DataFrame({
        "location_code": df["iso"],
        "indicator_code": "HF",
        "year": df["year"],
        "value": df["hf_score"],
    })

    return out


import re as _re

def epi(data) -> pd.DataFrame:
    df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)

    # Colunas de valor: INDICADOR.raw.ANO
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


import pandas as pd

def epi_indicadores(data):
    df = pd.DataFrame(data) if isinstance(data, list) else data

    skip_cols = {"code", "iso", "country", "name", "region"}

    indicator_cols = [c for c in df.columns if c not in skip_cols]

    # extrair indicador (antes de .raw.)
    indicators = {
        c.split(".raw.")[0] for c in indicator_cols if ".raw." in c
    }

    return pd.DataFrame({
        "code": [f"{ind}" for ind in indicators],
        "name": [None] * len(indicators)
    })


import re
import pandas as pd

import re
import pandas as pd

def epi_values(data):
    df = pd.DataFrame(data) if isinstance(data, list) else data

    iso_col = next((c for c in df.columns if "iso" in c.lower()), None)
    
    skip_cols = {iso_col}
    skip_cols.update(c for c in df.columns if "country" in c.lower() or "name" in c.lower())

    indicator_cols = [c for c in df.columns if c not in skip_cols]

    rows = []

    for _, row in df.iterrows():
        for col in indicator_cols:
            val = row[col]
            if pd.isna(val):
                continue

            match = re.search(r'(\d{4})$', col)
            year = int(match.group(1)) if match else None

            # extrair indicador correto
            indicator = col.split(".raw.")[0]

            rows.append({
                "location_code": row[iso_col],
                "indicator_code": f"{indicator}",
                "year": year,
                "value": float(val),
            })

    return pd.DataFrame(rows).dropna(subset=["location_code", "year"])


# ══════════════════════════════════════════════════════════════
# REGISTO DE FUNÇÕES  (auto-descoberta — todas as funções públicas do módulo)
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
# AUTO-GENERATED FUNCTIONS (carregadas de silver_functions_auto.json)
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
