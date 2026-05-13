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
    "dbname": "gestao_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

DB_PIPELINE = {
    "host": "localhost",
    "port": 5433,
    "dbname": "gestao_db",
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

def log_etl(step, error_message=None, file_id=None):
    cur_pipe.execute("""
        INSERT INTO etl_logs_dados (file_id, step, error_message)
        VALUES (%s, %s, %s)
    """, (str(file_id) if file_id is not None else None, step, error_message))

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

    report_map = {}

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            base = key.rsplit(".", 1)[0]

            try:
                metadata = get_object_metadata(key)
            except Exception as e:
                print(f"Erro ao ler metadata de {key}: {e}")
                log_etl("load_mapping",f"Erro metadata: {e}", file_id=base)
                continue

            created_at = metadata.get("created_at")
            raw_report_id = metadata.get("report_id")

            try:
                report_id = int(raw_report_id) if raw_report_id not in (None, "", "None") else None
                if report_id is None:
                    raise ValueError("report_id é None ou inválido")
            except (ValueError, TypeError) as e:
                log_etl("load_mapping",f"Erro metadata: report_id inválido ({raw_report_id}) | {e}", file_id=base)
                continue

            try:
                created_at_dt = parse_date(created_at)
                if created_at_dt.tzinfo is None:
                    created_at_dt = created_at_dt.replace(tzinfo=timezone.utc)
            except Exception as e:
                print(f"created_at inválido em {key}: {created_at}")
                log_etl("load_mapping",f"created_at inválido: {e}", file_id=base)
                continue

            if last_run is not None and created_at_dt <= last_run:
                continue

            report_map[base] = report_id

    return report_map


# ===============================
# LOAD DIMENSIONS
# ===============================
def load_dimensions(report_map):
    cur_op.execute("SELECT report_id, source_code FROM op_report")
    report_source_map = {r[0]: r[1] for r in cur_op.fetchall()}

    for nome_base, report_id in report_map.items():
        ficheiro = nome_base + ".parquet"
        print("Tentando ler ficheiro ", ficheiro)
        try:
            df = read_parquet_from_s3(ficheiro)
        except Exception as e:
            print(f"Erro ao ler {ficheiro}: {e}")
            log_etl('load',str(e), file_id=nome_base)
            continue

        if df.empty:
            log_etl('load',"DataFrame vazio", file_id=nome_base)
            continue

        if set(df.columns) != {"code", "name"}:
            continue
        source_system = report_source_map.get(report_id)
        if not source_system:
            log_etl('load',f"source_system não encontrado para report_id {report_id}", file_id=nome_base)
            continue

        dados = [(source_system, row['code'], row['name']) for _, row in df.iterrows()]
        psycopg2.extras.execute_values(cur_dw, """
            INSERT INTO dim_indicator (source_system, indicator_code, indicator_name)
            VALUES %s
            ON CONFLICT DO NOTHING
        """, dados, page_size=5000)

    conn_dw.commit()
    conn_pipe.commit()
    print("Dimensões carregadas")


# ===============================
# LOAD SOURCE + REPORTS (INCREMENTAL)
# ===============================
def load_source_and_reports():
    if last_run is not None:
        cur_op.execute("""
            SELECT report_id, source_code, report_url, publication_date
            FROM op_report
            WHERE created_at > %s;
        """, (last_run,))
    else:
        cur_op.execute("""
            SELECT report_id, source_code, report_url, publication_date
            FROM op_report;
        """)

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
# LOAD FACTS
# ===============================
def load_facts(report_map):
    cur_dw.execute("SELECT location_code, location_sk FROM dim_location")
    loc_map = {code: sk for code, sk in cur_dw.fetchall()}

    cur_dw.execute("SELECT source_system, indicator_code, indicator_sk FROM dim_indicator")
    ind_map = {(source, code): sk for source, code, sk in cur_dw.fetchall()}

    cur_dw.execute("SELECT year, date_id FROM dim_date")
    date_map = {year: date_id for year, date_id in cur_dw.fetchall()}

    cur_op.execute("SELECT report_id, source_code FROM op_report")
    report_source_map = {r[0]: r[1] for r in cur_op.fetchall()}

    for nome_base, report_id in report_map.items():
        ficheiro = nome_base + ".parquet"
        try:
            df = read_parquet_from_s3(ficheiro)
        except Exception as e:
            print(f"Erro ao ler {ficheiro}: {e}")
            log_etl('load',str(e), file_id=nome_base)
            continue

        if df.empty:
            log_etl('load',"DataFrame vazio", file_id=nome_base)
            continue

        if set(df.columns) != {"location_code", "indicator_code", "year", "value", "value_type"}:
            continue
        source_system = report_source_map.get(report_id)
        if not source_system:
            log_etl('load',f"source_system não encontrado para report_id {report_id}", file_id=nome_base)
            continue

        df['location_sk'] = df['location_code'].map(loc_map)
        df['indicator_sk'] = df.apply(
            lambda r: ind_map.get((source_system, r['indicator_code'])), axis=1
        )
        df['date_id'] = df['year'].map(date_map)

        valid_df = df[
            df['location_sk'].notna() &
            df['indicator_sk'].notna() &
            df['date_id'].notna()
        ].copy()

        if valid_df.empty:
            dropped = len(df)
            missing_loc = int(df['location_sk'].isna().sum())
            missing_ind = int(df['indicator_sk'].isna().sum())
            missing_date = int(df['date_id'].isna().sum())
            log_etl('load',
                    f"0 linhas válidas de {dropped} — "
                    f"location_sk em falta: {missing_loc}, "
                    f"indicator_sk em falta: {missing_ind}, "
                    f"date_id em falta: {missing_date}",
                    file_id=nome_base)
            continue

        valid_df['report_id']    = report_id
        valid_df['location_sk']  = valid_df['location_sk'].astype(int)
        valid_df['indicator_sk'] = valid_df['indicator_sk'].astype(int)
        valid_df['date_id']      = valid_df['date_id'].astype(int)
        valid_df['report_id']    = valid_df['report_id'].astype(int)

        insert_data = valid_df[['report_id', 'location_sk', 'indicator_sk', 'date_id', 'value', 'value_type']].values.tolist()

        psycopg2.extras.execute_values(cur_dw, """
            INSERT INTO fact_values (
                report_id,
                location_sk,
                indicator_sk,
                date_id,
                value,
                value_type
            )
            VALUES %s
            ON CONFLICT DO NOTHING
        """, insert_data, page_size=10000)

    conn_dw.commit()
    conn_pipe.commit()
    print("Fact table carregada (Em Bloco)")

# ===============================
# VIEWS
# ===============================
def ensure_views():
    cur_dw.execute("""
        CREATE OR REPLACE VIEW vw_indicator_location_year AS
        SELECT
            di.indicator_name,
            dl.name  AS location_name,
            fv.value,
            dd.year
        FROM fact_values fv
        JOIN dim_indicator di ON fv.indicator_sk = di.indicator_sk
        JOIN dim_location   dl ON fv.location_sk  = dl.location_sk
        JOIN dim_date        dd ON fv.date_id      = dd.date_id;
    """)
    conn_dw.commit()
    print("Views atualizadas")


# ===============================
# PIPELINE
# ===============================
def run_etl():
    report_map = get_mapping()

    if not report_map:
        print("Sem novos dados para carregar.")
        ensure_views()
        return

    print("1. Dimensões")
    load_dimensions(report_map)

    print("2. Source + Reports")
    load_source_and_reports()

    print("3. Facts")
    load_facts(report_map)

    print("4. Views")
    ensure_views()

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
