import psycopg2
import pandas as pd
import boto3
from io import BytesIO
from datetime import timezone
from dateutil.parser import parse as parse_date

# ===============================
# CONFIGURAÇÕES
# ===============================
MINIO_ENDPOINT = 'http://localhost:9000'
BUCKET_NAME = "transformed"

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "projeto_db",
    "user": "projeto_utilizador",
    "password": "projeto"
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

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# ===============================
# ETL CONTROLO
# ===============================
cur.execute("""
SELECT last_run
FROM etl_data
WHERE process_name = 'etl_main';
""")
last_run = cur.fetchone()[0]

# Garante que last_run tem timezone para comparação com metadata do MinIO
if last_run is not None and last_run.tzinfo is None:
    last_run = last_run.replace(tzinfo=timezone.utc)

def log_etl(file_name, step, status, error_message=None):
    cur.execute("""
        INSERT INTO etl_logs (file_name, step, status, error_message)
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
    """
    Devolve o dicionário de metadata de um objeto no MinIO.
    Os campos esperados na metadata são: report_id, file_type, created_at.
    A metadata do S3/MinIO é guardada em head_object sob a chave 'Metadata'
    e os nomes das chaves são sempre devolvidos em minúsculas.
    """
    response = s3.head_object(Bucket=BUCKET_NAME, Key=key)
    return response.get("Metadata", {})


# ===============================
# MAPPING (INCREMENTAL via metadata MinIO)
# ===============================
def get_mapping():
    """
    Lista todos os objetos .parquet no bucket e lê a metadata de cada um.
    Filtra apenas os ficheiros cujo created_at (metadata) seja posterior ao last_run.
    Devolve:
        mapping    -> { nome_base: file_type }
        report_map -> { nome_base: report_id }
    """
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME)

    mapping = {}
    report_map = {}

    for page in pages:

        for obj in page.get("Contents", []):

            key = obj["Key"]

            # if not key.endswith(".parquet"):
            #     continue

            # Extrai o nome base (sem extensão)
            base = key.rsplit(".", 1)[0]
            # if not base:
            #     print(f"Base vazia ignorada: {key}")
            #     log_etl(key, "load_mapping", "error", "Base vazia")
            #     continue

            # Lê metadata do objeto
            try:
                metadata = get_object_metadata(key)
            except Exception as e:
                print(f"Erro ao ler metadata de {key}: {e}")
                log_etl(key, "load_mapping", "error", f"Erro metadata: {e}")
                continue

            file_type  = metadata.get("file_type")
            created_at = metadata.get("created_at")

            # report_id vem sempre como string da metadata — converter para int
            raw_report_id = metadata.get("report_id")

            try:
                report_id = int(raw_report_id) if raw_report_id not in (None, "", "None") else None
                
                # 🚨 FORÇAR ERRO se for None
                if report_id is None:
                    raise ValueError("report_id é None ou inválido")

            except (ValueError, TypeError) as e:
                log_etl(key, "load_mapping", "error", f"Erro metadata: report_id inválido ({raw_report_id}) | {e}")
                continue

            # # Valida campos obrigatórios
            # if not file_type or not created_at:
            #     print(f"Metadata incompleta ignorada: {key} -> {metadata}")
            #     log_etl(key, "load_mapping", "error", "Metadata incompleta (file_type/created_at em falta)")
            #     continue

            # Filtro incremental: apenas ficheiros mais recentes que last_run
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

        # Inserção de Indicadores
        if file_type == 'indicator':
            query = """
                INSERT INTO dim_indicator (indicator_code, indicator_name)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            # Converte o dataframe para uma lista de tuplos instantaneamente
            dados = df[['code', 'name']].values.tolist() 
            psycopg2.extras.execute_values(cur, query, dados, page_size=5000)

        # LOCATIONS
        elif file_type in ['countries', 'regions', 'groups']:
            for code, name in df.itertuples(index=False):
                cur.execute("""
                    INSERT INTO dim_location (location_code, location_name)
                    VALUES (%s,%s)
                    ON CONFLICT DO NOTHING
                """, (code, name))

    conn.commit()
    print("Dimensões carregadas")


# ===============================
# LOAD SOURCE + REPORTS (INCREMENTAL)
# ===============================
def load_source_and_reports():

    cur.execute("""
        SELECT report_id, source_code, report_url, publication_date
        FROM op_report
        WHERE created_at > %s;
    """, (last_run,))

    rows = cur.fetchall()

    for report_id, source_code, report_url, publication_date in rows:

        # SOURCE
        if source_code:
            cur.execute("""
                INSERT INTO dim_source (source_code, source_name)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (source_code, source_code))

        # REPORT
        cur.execute("""
            INSERT INTO dim_report (
                report_id,
                source_code,
                report_url,
                publication_date
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (report_id) DO NOTHING
        """, (
            report_id,
            source_code,
            report_url,
            publication_date
        ))

    conn.commit()
    print("Source + Reports carregados")


# ===============================
# LOAD DATE (STATIC)
# ===============================
def load_date():
    for year in range(1750, 2041):
        cur.execute("""
            INSERT INTO dim_date (year)
            VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (year,))

    conn.commit()


# ===============================
# LOAD FACTS
# ===============================
def load_facts(mapping, report_map):

    # 1. Carregar caches para a memória
    cur.execute("SELECT location_code FROM dim_location")
    loc_map = set(r[0] for r in cur.fetchall())

    cur.execute("SELECT indicator_code FROM dim_indicator")
    ind_map = set(r[0] for r in cur.fetchall())

    # Alterado para facilitar o mapeamento direto no Pandas (year como chave, date_id como valor)
    cur.execute("SELECT year, date_id FROM dim_date")
    date_map = {year: date_id for year, date_id in cur.fetchall()}

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

        # ==========================================
        # 2. FILTRAGEM E TRANSFORMAÇÃO VETORIAL
        # ==========================================
        
        # Mapeia o ano para o date_id usando o dicionário da cache
        df['date_id'] = df['year'].map(date_map)
        
        # Mantém apenas as linhas onde o código existe na cache (usando .isin)
        valid_df = df[
            (df['location_code'].isin(loc_map)) &
            (df['indicator_code'].isin(ind_map)) &
            (df['date_id'].notna())
        ].copy()

        if valid_df.empty:
            continue
            
        # Adiciona a coluna report_id
        valid_df['report_id'] = report_id

        # Garante o formato inteiro para evitar erros no Postgres
        valid_df['date_id'] = valid_df['date_id'].astype(int)
        valid_df['report_id'] = valid_df['report_id'].astype(int)

        # Prepara a matriz de dados na ordem exata do INSERT
        insert_data = valid_df[['report_id', 'location_code', 'indicator_code', 'date_id', 'value', 'value_type']].values.tolist()

        # ==========================================
        # 3. INSERÇÃO EM BLOCO (BATCH INSERT)
        # ==========================================
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
        
        # Envia milhares de linhas de uma só vez para o Postgres
        psycopg2.extras.execute_values(
            cur, 
            query, 
            insert_data, 
            page_size=10000
        )

    conn.commit()
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
        cur.close()
        conn.close()