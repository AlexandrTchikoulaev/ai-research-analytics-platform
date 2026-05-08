import json
import io
import pandas as pd
import boto3
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "pipeline_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

BUCKET_RAW = "bronze"
BUCKET_SILVER = "silver"
PROCESS_NAME = "etl_dados"

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE TRANSFORMAÇÃO — IMF
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
# Formato típico: CSV wide-to-long
# Colunas esperadas: ISO_code, countries, region, year, hf_score, ...
# ══════════════════════════════════════════════════════════════

def funcao_hfi_countries(data):
    df = pd.DataFrame(data) if isinstance(data, list) else data
    result = df[["ISO_code", "countries"]].drop_duplicates().rename(
        columns={"ISO_code": "code", "countries": "name"}
    )
    return result.dropna(subset=["code", "name"])


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


# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE TRANSFORMAÇÃO — EPI (Environmental Performance Index)
# Formato típico: CSV com country, iso, year e colunas de indicadores
# ══════════════════════════════════════════════════════════════

def funcao_epi_countries(data):
    df = pd.DataFrame(data) if isinstance(data, list) else data
    iso_col = next((c for c in df.columns if "iso" in c.lower()), None)
    name_col = next((c for c in df.columns if "country" in c.lower() or "name" in c.lower()), None)
    if not iso_col or not name_col:
        return pd.DataFrame(columns=["code", "name"])
    return df[[iso_col, name_col]].drop_duplicates().rename(
        columns={iso_col: "code", name_col: "name"}
    ).dropna()


def funcao_epi_indicadores(data):
    df = pd.DataFrame(data) if isinstance(data, list) else data
    skip = {"iso", "country", "name", "region", "year"}
    indicator_cols = [c for c in df.columns if c.lower() not in skip]
    return pd.DataFrame({
        "code": [f"EPI_{c}" for c in indicator_cols],
        "name": indicator_cols,
    })


def funcao_epi_values(data):
    df = pd.DataFrame(data) if isinstance(data, list) else data
    iso_col = next((c for c in df.columns if "iso" in c.lower()), None)
    year_col = next((c for c in df.columns if "year" in c.lower()), None)
    skip = {iso_col, year_col, "country", "name", "region"}
    indicator_cols = [c for c in df.columns if c not in skip and c is not None]

    rows = []
    for _, row in df.iterrows():
        for ind in indicator_cols:
            val = row.get(ind)
            if pd.isna(val):
                continue
            rows.append({
                "location_code": row.get(iso_col),
                "indicator_code": f"EPI_{ind}",
                "year": int(row.get(year_col)) if year_col and not pd.isna(row.get(year_col)) else None,
                "value": float(val),
                "value_type": "value",
            })

    df_out = pd.DataFrame(rows)
    return df_out.dropna(subset=["location_code", "indicator_code", "year"])


# ══════════════════════════════════════════════════════════════
# REGISTO DE FUNÇÕES
# ══════════════════════════════════════════════════════════════

EXTRACT_FUNCTIONS = {
    "funcao_imf_indicadores": funcao_imf_indicadores,
    "funcao_imf_countries":   funcao_imf_countries,
    "funcao_imf_regions":     funcao_imf_regions,
    "funcao_imf_groups":      funcao_imf_groups,
    "funcao_imf_values":      funcao_imf_values,
    "funcao_hfi_countries":   funcao_hfi_countries,
    "funcao_hfi_indicadores": funcao_hfi_indicadores,
    "funcao_hfi_values":      funcao_hfi_values,
    "funcao_epi_countries":   funcao_epi_countries,
    "funcao_epi_indicadores": funcao_epi_indicadores,
    "funcao_epi_values":      funcao_epi_values,
}

# ══════════════════════════════════════════════════════════════
# LIMPEZA
# ══════════════════════════════════════════════════════════════

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    if "code" in df.columns:
        df = df.dropna(subset=["code"])
    if "location_code" in df.columns:
        df = df.dropna(subset=["location_code", "indicator_code", "year"])
    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════
# LEITURA DE DADOS BRUTOS
# ══════════════════════════════════════════════════════════════

def read_raw_object(s3, key: str, fmt: str):
    response = s3.get_object(Bucket=BUCKET_RAW, Key=key)
    content = response["Body"].read()
    if fmt == "json":
        return json.loads(content)
    elif fmt == "csv":
        return pd.read_csv(io.BytesIO(content))
    elif fmt in ("excel", "xlsx", "xls"):
        return pd.read_excel(io.BytesIO(content))
    else:
        # Tentar JSON por defeito
        return json.loads(content)


# ══════════════════════════════════════════════════════════════
# PIPELINE DE TRANSFORMAÇÃO
# ══════════════════════════════════════════════════════════════

def transformar():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    s3 = boto3.client("s3", **MINIO_CONFIG)

    # Garantir bucket transformed
    try:
        s3.head_bucket(Bucket=BUCKET_SILVER)
    except Exception:
        s3.create_bucket(Bucket=BUCKET_SILVER)

    # last_run
    cur.execute("SELECT last_run FROM etl_data WHERE process_name = %s", (PROCESS_NAME,))
    row = cur.fetchone()
    last_run = row[0] if row else None

    # Listar objetos no bucket raw
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_RAW)

    ok_count = 0
    err_count = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            try:
                head = s3.head_object(Bucket=BUCKET_RAW, Key=key)
                metadata = head.get("Metadata", {})
            except Exception as e:
                print(f"[ERRO] Metadata de {key}: {e}")
                continue

            extract_function = metadata.get("extract_function", "")
            file_type = metadata.get("file_type", "")
            file_name = metadata.get("file_name", key)
            created_at_str = metadata.get("created_at", "")
            fmt = metadata.get("file_format", "json")

            # Filtro incremental
            if last_run and created_at_str:
                try:
                    from dateutil.parser import parse as parse_date
                    from datetime import timezone
                    created_dt = parse_date(created_at_str)
                    if created_dt.tzinfo is None:
                        created_dt = created_dt.replace(tzinfo=timezone.utc)
                    lr = last_run
                    if lr.tzinfo is None:
                        lr = lr.replace(tzinfo=timezone.utc)
                    if created_dt <= lr:
                        continue
                except Exception:
                    pass

            if not extract_function:
                print(f"[SKIP] Sem extract_function: {key}")
                continue

            if extract_function not in EXTRACT_FUNCTIONS:
                print(f"[SKIP] Função desconhecida '{extract_function}': {key}")
                cur.execute("""
                    INSERT INTO etl_logs_dados (file_name, step, status, error_message)
                    VALUES (%s, %s, %s, %s)
                """, (file_name, "transform", "error", f"Função desconhecida: {extract_function}"))
                err_count += 1
                continue

            print(f"A transformar: {key} ({extract_function})")

            try:
                data = read_raw_object(s3, key, fmt)
                funcao = EXTRACT_FUNCTIONS[extract_function]

                # Funções que recebem DataFrame já passam df, as JSON recebem dict
                if isinstance(data, pd.DataFrame):
                    df = funcao(data)
                else:
                    df = funcao(data)

                if df is None or df.empty:
                    raise ValueError("DataFrame vazio após transformação")

                df = clean_dataframe(df)

                # Serializar para Parquet
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)

                out_key = f"{key}.parquet"

                s3.put_object(
                    Bucket=BUCKET_SILVER,
                    Key=out_key,
                    Body=buffer.getvalue(),
                    Metadata=metadata,  # preservar toda a metadata do raw
                )
                print(f"[OK]   {key} -> {out_key}")
                ok_count += 1

            except Exception as e:
                cur.execute("""
                    INSERT INTO etl_logs_dados (file_name, step, status, error_message)
                    VALUES (%s, %s, %s, %s)
                """, (file_name, "transform", "error", str(e)))
                print(f"[ERRO] {key}: {e}")
                err_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"transform concluído — {ok_count} OK, {err_count} erros")


if __name__ == "__main__":
    transformar()
