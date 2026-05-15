import pandas as pd

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
            "value_type": "value",
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
        "value_type": "value"
    })

    return out


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
                "value_type": "value",
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
    return df.reset_index(drop=True)
