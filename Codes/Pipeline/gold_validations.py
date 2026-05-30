"""
Valida os ficheiros Parquet no bucket Silver correspondentes a ficheiros
com pipeline_status = 'SILVER_OK'.
Actualiza pipeline_status e regista erros em etl_logs_dados.
"""
import io
import psycopg2
import boto3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import DB_CONFIG, MINIO_CONFIG, BUCKET_SILVER

_REQUIRED_COLS = {"location_code", "indicator_code", "indicator_name", "year", "value"}

MAX_WORKERS = 8


def _validate_one(file_id: int) -> bool:
    """Valida um Parquet Silver num thread dedicado."""
    s3 = boto3.client("s3", **MINIO_CONFIG)
    key = f"{file_id}.parquet"
    errors = []

    try:
        response = s3.get_object(Bucket=BUCKET_SILVER, Key=key)
        buf = io.BytesIO(response["Body"].read())
        df = pd.read_parquet(buf, engine="pyarrow")
    except Exception as e:
        errors.append(f"Erro ao ler Parquet: {e}")
        df = None

    if df is not None:
        if df.empty:
            errors.append("Ficheiro Parquet está vazio")
        else:
            missing = _REQUIRED_COLS - set(df.columns)
            if missing:
                errors.append(f"Colunas em falta: {sorted(missing)}")
            else:
                for col in ("location_code", "indicator_code", "year"):
                    null_count = int(df[col].isna().sum())
                    if null_count > 0:
                        errors.append(f"'{col}' tem {null_count} valor(es) nulo(s)")

                # Limiar de 80%: tolera edge cases (WLD, EMU, etc.) mas rejeita dados sem ISO3
                sample_locs = df["location_code"].dropna().astype(str).head(30).tolist()
                if sample_locs:
                    iso3_count = sum(1 for v in sample_locs if len(v) == 3 and v.isalpha() and v.isupper())
                    if iso3_count < max(1, len(sample_locs) * 0.8):
                        bad = [v for v in sample_locs if not (len(v) == 3 and v.isalpha() and v.isupper())][:5]
                        errors.append(
                            f"'location_code' não contém códigos ISO3 válidos (ex: 'AFG','PRT'). "
                            f"Encontrado: {bad}"
                        )

                invalid_years = int(pd.to_numeric(df["year"], errors="coerce").isna().sum())
                if invalid_years > 0:
                    errors.append(f"'year' tem {invalid_years} valor(es) não numérico(s)")

                valid_values = int(pd.to_numeric(df["value"], errors="coerce").notna().sum())
                if valid_values == 0:
                    errors.append("'value' não tem nenhum valor numérico válido")

    if errors:
        msg = "; ".join(errors)
        print(f"[INVÁLIDO] file_id={file_id}: {msg}")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = %s WHERE file_id = %s",
                (msg, file_id)
            )
            cur.execute(
                "INSERT INTO etl_logs_dados (file_id, step, error_message) VALUES (%s, %s, %s)",
                (file_id, "validate_silver", msg)
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        return False

    return True


def validate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT file_id FROM op_data WHERE pipeline_status = 'SILVER_OK'")
    file_ids = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()

    ok_count = 0
    err_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_validate_one, file_id): file_id
            for file_id in file_ids
        }
        for future in as_completed(futures):
            try:
                if future.result():
                    ok_count += 1
                else:
                    err_count += 1
            except Exception as e:
                file_id = futures[future]
                print(f"[ERRO] file_id={file_id} exceção não tratada: {e}")
                err_count += 1

    print(f"validate_silver — {ok_count} válidos, {err_count} inválidos")


if __name__ == "__main__":
    validate()
