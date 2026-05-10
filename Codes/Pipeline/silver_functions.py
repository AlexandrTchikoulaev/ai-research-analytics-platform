import pandas as pd

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE TRANSFORMAÇÃO — IMF (formato JSON compacto)
# Estrutura: {"indicators":{}, "countries":{}, "values":{ind:{loc:{year:val}}}}
# ══════════════════════════════════════════════════════════════

def funcao_imf_indicadores(data):
    items = data.get("indicators", {})
    return pd.DataFrame({
        "code": list(items.keys()),
        "name": [v.get("label") for v in items.values()],
    })


def funcao_imf_countries(data):
    items = data.get("countries", {})
    return pd.DataFrame({
        "code": list(items.keys()),
        "name": [v.get("label") for v in items.values()],
    })


def funcao_imf_regions(data):
    items = data.get("regions", {})
    return pd.DataFrame({
        "code": list(items.keys()),
        "name": [v.get("label") for v in items.values()],
    })


def funcao_imf_groups(data):
    items = data.get("groups", {})
    return pd.DataFrame({
        "code": list(items.keys()),
        "name": [v.get("label") for v in items.values()],
    })


def funcao_imf_values(data):
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
        for loc, years in locations.items()
        for year, val in years.items()
    ]
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE TRANSFORMAÇÃO — HFI (Human Freedom Index)
# Formato: CSV long com colunas ISO_code, countries, region, year, indicadores...
# ══════════════════════════════════════════════════════════════

def funcao_hfi_countries(data):
    df = pd.DataFrame(data) if isinstance(data, list) else data
    result = df[["ISO_code", "countries"]].drop_duplicates().rename(
        columns={"ISO_code": "code", "countries": "name"}
    )
    return result.dropna(subset=["code", "name"])


def funcao_hfi_indicadores(data):
    df = pd.DataFrame(data) if isinstance(data, list) else data
    indicator_cols = [
        c for c in df.columns
        if c not in ("ISO_code", "countries", "region", "year", "rank")
    ]
    return pd.DataFrame({
        "code": [f"HFI_{c}" for c in indicator_cols],
        "name": indicator_cols,
    })


def funcao_hfi_values(data):
    df = pd.DataFrame(data) if isinstance(data, list) else data
    indicator_cols = [
        c for c in df.columns
        if c not in ("ISO_code", "countries", "region", "year", "rank")
    ]

    rows = []
    for _, row in df.iterrows():
        for ind in indicator_cols:
            val = row.get(ind)
            if pd.isna(val):
                continue
            rows.append({
                "location_code": row.get("ISO_code"),
                "indicator_code": f"HFI_{ind}",
                "year": int(row.get("year")) if not pd.isna(row.get("year")) else None,
                "value": float(val),
                "value_type": "value",
            })

    df_out = pd.DataFrame(rows)
    return df_out.dropna(subset=["location_code", "indicator_code", "year"])


# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE TRANSFORMAÇÃO — EPI (Environmental Performance Index)
# Formato: CSV com country, iso, year e colunas de indicadores
# ══════════════════════════════════════════════════════════════

def funcao_epi_countries(data):
    df = pd.DataFrame(data) if isinstance(data, list) else data
    
    iso_col  = next((c for c in df.columns if "iso" in c.lower()), None)
    name_col = next((c for c in df.columns if "country" in c.lower() or "name" in c.lower()), None)
    
    if not iso_col or not name_col:
        return pd.DataFrame(columns=["code", "name"])
    
    return (
        df[[iso_col, name_col]]
        .dropna(subset=[name_col])   # <-- só remove se name for nulo
        .drop_duplicates()
        .rename(columns={iso_col: "code", name_col: "name"})
    )


import pandas as pd

def funcao_epi_indicadores(data):
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

def funcao_epi_values(data):
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
# REGISTO DE FUNÇÕES
# ══════════════════════════════════════════════════════════════

EXTRACT_FUNCTIONS = {
    # IMF JSON compacto
    "funcao_imf_indicadores": funcao_imf_indicadores,
    "funcao_imf_values":      funcao_imf_values,

    # HFI
    "funcao_hfi_indicadores": funcao_hfi_indicadores,
    "funcao_hfi_values":      funcao_hfi_values,

    # EPI
    "funcao_epi_indicadores": funcao_epi_indicadores,
    "funcao_epi_values":      funcao_epi_values,
}

# Mapeamento função → file_type (adicionar aqui ao registar uma nova função)
FUNCTION_FILE_TYPE = {
    "funcao_imf_indicadores": "indicator",
    "funcao_imf_values":      "value",
    "funcao_hfi_indicadores": "indicator",
    "funcao_hfi_values":      "value",
    "funcao_epi_indicadores": "indicator",
    "funcao_epi_values":      "value",
}


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
