import psycopg2
import psycopg2.extras
import pandas as pd
import boto3
from io import BytesIO
from datetime import timezone
from dateutil.parser import parse as parse_date

# ===============================
# CONFIGURAÇÕES
# ===============================
MINIO_ENDPOINT = 'http://localhost:9002'
BUCKET_NAME = "silver"

DB_WAREHOUSE = {
    "host": "localhost",
    "port": 5433,
    "dbname": "warehouse_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

DB_OPERATIONAL = {
    "host": "localhost",
    "port": 5433,
    "dbname": "operational_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

DB_PIPELINE = {
    "host": "localhost",
    "port": 5433,
    "dbname": "pipeline_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

# ===============================
# CONEXÕES
# ===============================
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id='admin',
    aws_secret_access_key='admin123'
)

conn_pipe = psycopg2.connect(**DB_PIPELINE)
cur_pipe  = conn_pipe.cursor()

conn_op = psycopg2.connect(**DB_OPERATIONAL)
cur_op  = conn_op.cursor()

conn_dw = psycopg2.connect(**DB_WAREHOUSE)
cur_dw  = conn_dw.cursor()

# ===============================
# ETL CONTROLO
# ===============================
cur_pipe.execute("""
SELECT last_run
FROM etl_data
WHERE process_name = 'etl_dados';
""")
last_run = cur_pipe.fetchone()[0]

if last_run is not None and last_run.tzinfo is None:
    last_run = last_run.replace(tzinfo=timezone.utc)

def log_etl(file_name, step, status, error_message=None):
    cur_pipe.execute("""
        INSERT INTO etl_logs_dados (file_name, step, status, error_message)
        VALUES (%s, %s, %s, %s)
    """, (file_name, step, status, error_message))

# ===============================
# HELPERS
# ===============================
def read_parquet_from_s3(key):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    buffer = BytesIO(response['Body'].read())
    return pd.read_parquet(buffer, engine='pyarrow')


def get_object_metadata(key):
    response = s3.head_object(Bucket=BUCKET_NAME, Key=key)
    return response.get("Metadata", {})


# ===============================
# MAPPING (INCREMENTAL via metadata MinIO)
# ===============================
def get_mapping():
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME)

    mapping = {}
    report_map = {}

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            base = key.rsplit(".", 1)[0]

            try:
                metadata = get_object_metadata(key)
            except Exception as e:
                print(f"Erro ao ler metadata de {key}: {e}")
                log_etl(key, "load_mapping", "error", f"Erro metadata: {e}")
                continue

            file_type  = metadata.get("file_type")
            created_at = metadata.get("created_at")
            raw_report_id = metadata.get("report_id")

            try:
                report_id = int(raw_report_id) if raw_report_id not in (None, "", "None") else None
                if report_id is None:
                    raise ValueError("report_id é None ou inválido")
            except (ValueError, TypeError) as e:
                log_etl(key, "load_mapping", "error", f"Erro metadata: report_id inválido ({raw_report_id}) | {e}")
                continue

            try:
                created_at_dt = parse_date(created_at)
                if created_at_dt.tzinfo is None:
                    created_at_dt = created_at_dt.replace(tzinfo=timezone.utc)
            except Exception as e:
                print(f"created_at inválido em {key}: {created_at}")
                log_etl(key, "load_mapping", "error", f"created_at inválido: {e}")
                continue

            if last_run is not None and created_at_dt <= last_run:
                continue

            mapping[base] = file_type
            report_map[base] = report_id

    return mapping, report_map


# ===============================
# LOAD DIMENSIONS
# ===============================
def load_dimensions(mapping):
    for nome_base, file_type in mapping.items():
        ficheiro = nome_base + ".parquet"
        print("Tentando ler ficheiro ", ficheiro)
        try:
            df = read_parquet_from_s3(ficheiro)
        except Exception as e:
            print(f"Erro ao ler {ficheiro}: {e}")
            log_etl(ficheiro, 'load', 'error', str(e))
            continue

        if df.empty:
            log_etl(ficheiro, 'load', 'error', "DataFrame vazio")
            continue

        if file_type == 'indicator':
            query = """
                INSERT INTO dim_indicator (indicator_code, indicator_name)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            dados = df[['code', 'name']].values.tolist()
            psycopg2.extras.execute_values(cur_dw, query, dados, page_size=5000)

        elif file_type in ['countries', 'regions', 'groups']:
            for code, name in df.itertuples(index=False):
                cur_dw.execute("""
                    INSERT INTO dim_location (location_code, location_name)
                    VALUES (%s,%s)
                    ON CONFLICT DO NOTHING
                """, (code, name))

    conn_dw.commit()
    conn_pipe.commit()
    print("Dimensões carregadas")


# ===============================
# LOAD SOURCE + REPORTS (INCREMENTAL)
# ===============================
def load_source_and_reports():
    cur_op.execute("""
        SELECT report_id, source_code, report_url, publication_date
        FROM op_report
        WHERE created_at > %s;
    """, (last_run,))

    rows = cur_op.fetchall()

    for report_id, source_code, report_url, publication_date in rows:
        cur_dw.execute("""
            INSERT INTO dim_report (
                report_id,
                source_code,
                source_name,
                report_url,
                publication_date
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (report_id) DO NOTHING
        """, (report_id, source_code, source_code, report_url, publication_date))

    conn_dw.commit()
    print("Source + Reports carregados")


# ===============================
# LOAD DATE (STATIC)
# ===============================
def load_date():
    for year in range(1750, 2041):
        cur_dw.execute("""
            INSERT INTO dim_date (year)
            VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (year,))

    conn_dw.commit()


# ===============================
# LOAD FACTS
# ===============================
def load_facts(mapping, report_map):
    cur_dw.execute("SELECT location_code FROM dim_location")
    loc_map = set(r[0] for r in cur_dw.fetchall())

    cur_dw.execute("SELECT indicator_code FROM dim_indicator")
    ind_map = set(r[0] for r in cur_dw.fetchall())

    cur_dw.execute("SELECT year, date_id FROM dim_date")
    date_map = {year: date_id for year, date_id in cur_dw.fetchall()}

    for nome_base, file_type in mapping.items():
        if file_type != 'value':
            continue
        if nome_base not in report_map:
            continue

        ficheiro = nome_base + ".parquet"
        try:
            df = read_parquet_from_s3(ficheiro)
        except Exception as e:
            print(f"Erro ao ler {ficheiro}: {e}")
            log_etl(ficheiro, 'load', 'error', str(e))
            continue

        if df.empty:
            log_etl(ficheiro, 'load', 'error', "DataFrame vazio")
            continue

        report_id = report_map[nome_base]

        df['date_id'] = df['year'].map(date_map)
        valid_df = df[
            (df['location_code'].isin(loc_map)) &
            (df['indicator_code'].isin(ind_map)) &
            (df['date_id'].notna())
        ].copy()

        if valid_df.empty:
            continue

        valid_df['report_id'] = report_id
        valid_df['date_id'] = valid_df['date_id'].astype(int)
        valid_df['report_id'] = valid_df['report_id'].astype(int)

        insert_data = valid_df[['report_id', 'location_code', 'indicator_code', 'date_id', 'value', 'value_type']].values.tolist()

        query = """
            INSERT INTO fact_values (
                report_id,
                location_code,
                indicator_code,
                date_id,
                value,
                value_type
            )
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        psycopg2.extras.execute_values(cur_dw, query, insert_data, page_size=10000)

    conn_dw.commit()
    conn_pipe.commit()
    print("Fact table carregada (Em Bloco)")

# ===============================
# PIPELINE
# ===============================
def run_etl():
    mapping, report_map = get_mapping()

    if not mapping:
        print("Sem novos dados para carregar.")
        return

    print("1. Dimensões")
    load_dimensions(mapping)

    print("2. Datas")
    load_date()

    print("3. Source + Reports")
    load_source_and_reports()

    print("4. Facts")
    load_facts(mapping, report_map)

    print("ETL concluído com sucesso")


# ===============================
# EXECUÇÃO
# ===============================
if __name__ == "__main__":
    try:
        run_etl()
    finally:
        cur_pipe.close()
        conn_pipe.close()
        cur_op.close()
        conn_op.close()
        cur_dw.close()
        conn_dw.close()
