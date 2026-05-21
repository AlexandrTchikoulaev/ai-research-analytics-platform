import psycopg2
import psycopg2.extras
import pandas as pd
import boto3
from io import BytesIO

DB_WAREHOUSE = {
    "host": "localhost", "port": 5433, "dbname": "warehouse_db",
    "user": "projeto_utilizador", "password": "projeto",
}

DB_OPERATIONAL = {
    "host": "localhost", "port": 5433, "dbname": "gestao_db",
    "user": "projeto_utilizador", "password": "projeto",
}

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

BUCKET_NAME = "silver"


def read_parquet_from_s3(s3, key):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    buffer = BytesIO(response["Body"].read())
    return pd.read_parquet(buffer, engine="pyarrow")


def get_mapping(cur_op):
    """Retorna {str(file_id): report_id} para ficheiros SILVER_OK."""
    cur_op.execute("SELECT file_id, report_id FROM op_data WHERE pipeline_status = 'SILVER_OK'")
    return {str(r[0]): r[1] for r in cur_op.fetchall()}


def load_dimensions(s3, report_map, cur_op, cur_dw, conn_dw, cur_pipe, conn_pipe, log_etl):
    cur_op.execute("SELECT report_id, source_code FROM op_report")
    report_source_map = {r[0]: r[1] for r in cur_op.fetchall()}

    for nome_base, report_id in report_map.items():
        ficheiro = nome_base + ".parquet"
        print("Tentando ler ficheiro ", ficheiro)
        try:
            df = read_parquet_from_s3(s3, ficheiro)
        except Exception as e:
            print(f"Erro ao ler {ficheiro}: {e}")
            log_etl("load", str(e), file_id=nome_base)
            continue

        if df.empty:
            log_etl("load", "DataFrame vazio", file_id=nome_base)
            continue

        cols = set(df.columns)
        if cols == {"code", "name"}:
            ind_pairs = [(row["code"], row["name"]) for _, row in df.iterrows()]
        elif cols == {"location_code", "indicator_code", "indicator_name", "year", "value"}:
            ind_pairs = (
                df[["indicator_code", "indicator_name"]]
                .drop_duplicates()
                .values.tolist()
            )
        else:
            continue

        source_system = report_source_map.get(report_id)
        if not source_system:
            log_etl("load", f"source_system não encontrado para report_id {report_id}", file_id=nome_base)
            continue

        dados = [(source_system, code, name) for code, name in ind_pairs]
        psycopg2.extras.execute_values(cur_dw, """
            INSERT INTO dim_indicator (source_system, indicator_code, indicator_name)
            VALUES %s
            ON CONFLICT DO NOTHING
        """, dados, page_size=5000)

    conn_dw.commit()
    conn_pipe.commit()
    print("Dimensões carregadas")


def load_source_and_reports(cur_op, cur_dw, conn_dw):
    """Carrega reports dos ficheiros SILVER_OK ou DONE."""
    cur_op.execute("""
        SELECT DISTINCT r.report_id, r.source_code, r.report_url, r.publication_date
        FROM op_report r
        JOIN op_data d ON d.report_id = r.report_id
        WHERE d.pipeline_status IN ('SILVER_OK', 'DONE')
    """)
    rows = cur_op.fetchall()

    for report_id, source_code, report_url, publication_date in rows:
        cur_dw.execute("""
            INSERT INTO dim_report (
                report_id, source_code, source_name, report_url, publication_date
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (report_id) DO NOTHING
        """, (report_id, source_code, source_code, report_url, publication_date))

    conn_dw.commit()
    print("Source + Reports carregados")


def load_facts(s3, report_map, cur_op, cur_dw, conn_dw, conn_pipe, log_etl):
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
            df = read_parquet_from_s3(s3, ficheiro)
        except Exception as e:
            print(f"Erro ao ler {ficheiro}: {e}")
            log_etl("load", str(e), file_id=nome_base)
            continue

        if df.empty:
            log_etl("load", "DataFrame vazio", file_id=nome_base)
            continue

        cols = set(df.columns)
        if cols not in (
            {"location_code", "indicator_code", "year", "value"},
            {"location_code", "indicator_code", "indicator_name", "year", "value"},
        ):
            continue
        source_system = report_source_map.get(report_id)
        if not source_system:
            log_etl("load", f"source_system não encontrado para report_id {report_id}", file_id=nome_base)
            continue

        df["location_sk"] = df["location_code"].map(loc_map)
        df["indicator_sk"] = df.apply(
            lambda r: ind_map.get((source_system, r["indicator_code"])), axis=1
        )
        df["date_id"] = df["year"].map(date_map)

        valid_df = df[
            df["location_sk"].notna() &
            df["indicator_sk"].notna() &
            df["date_id"].notna()
        ].copy()

        if valid_df.empty:
            dropped = len(df)
            missing_loc  = int(df["location_sk"].isna().sum())
            missing_ind  = int(df["indicator_sk"].isna().sum())
            missing_date = int(df["date_id"].isna().sum())
            log_etl("load",
                    f"0 linhas válidas de {dropped} — "
                    f"location_sk em falta: {missing_loc}, "
                    f"indicator_sk em falta: {missing_ind}, "
                    f"date_id em falta: {missing_date}",
                    file_id=nome_base)
            continue

        valid_df["report_id"]    = report_id
        valid_df["location_sk"]  = valid_df["location_sk"].astype(int)
        valid_df["indicator_sk"] = valid_df["indicator_sk"].astype(int)
        valid_df["date_id"]      = valid_df["date_id"].astype(int)
        valid_df["report_id"]    = valid_df["report_id"].astype(int)

        insert_data = valid_df[["report_id", "location_sk", "indicator_sk", "date_id", "value"]].values.tolist()

        psycopg2.extras.execute_values(cur_dw, """
            INSERT INTO fact_values (
                report_id, location_sk, indicator_sk, date_id, value
            )
            VALUES %s
            ON CONFLICT DO NOTHING
        """, insert_data, page_size=10000)

    conn_dw.commit()
    conn_pipe.commit()
    print("Fact table carregada (Em Bloco)")


def ensure_views(cur_dw, conn_dw):
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


def run_etl():
    s3 = boto3.client("s3", endpoint_url=MINIO_CONFIG["endpoint_url"],
                      aws_access_key_id=MINIO_CONFIG["aws_access_key_id"],
                      aws_secret_access_key=MINIO_CONFIG["aws_secret_access_key"])

    conn_pipe = psycopg2.connect(**DB_OPERATIONAL)
    cur_pipe  = conn_pipe.cursor()

    conn_op = psycopg2.connect(**DB_OPERATIONAL)
    cur_op  = conn_op.cursor()

    conn_dw = psycopg2.connect(**DB_WAREHOUSE)
    cur_dw  = conn_dw.cursor()

    def log_etl(step, error_message=None, file_id=None):
        cur_pipe.execute("""
            INSERT INTO etl_logs_dados (file_id, step, error_message)
            VALUES (%s, %s, %s)
        """, (str(file_id) if file_id is not None else None, step, error_message))

    try:
        report_map = get_mapping(cur_op)

        if not report_map:
            print("Sem novos dados para carregar.")
            ensure_views(cur_dw, conn_dw)
            return

        processed_ids = set()

        print("1. Dimensões")
        load_dimensions(s3, report_map, cur_op, cur_dw, conn_dw, cur_pipe, conn_pipe, log_etl)

        print("2. Source + Reports")
        load_source_and_reports(cur_op, cur_dw, conn_dw)

        print("3. Facts")
        load_facts(s3, report_map, cur_op, cur_dw, conn_dw, conn_pipe, log_etl)

        # Marcar DONE todos os ficheiros do report_map que foram processados sem erro grave
        for nome_base in report_map:
            file_id = int(nome_base)
            cur_op.execute(
                "UPDATE op_data SET pipeline_status = 'DONE' WHERE file_id = %s AND pipeline_status = 'SILVER_OK'",
                (file_id,)
            )
            processed_ids.add(file_id)
        conn_op.commit()

        # Ficheiros do report_map que não chegaram a DONE ficam como FAILED
        for nome_base, _ in report_map.items():
            file_id = int(nome_base)
            if file_id not in processed_ids:
                cur_op.execute(
                    "UPDATE op_data SET pipeline_status = 'FAILED', pipeline_error = 'Não processado no load' WHERE file_id = %s",
                    (file_id,)
                )
        conn_op.commit()

        print("4. Views")
        ensure_views(cur_dw, conn_dw)

        print("ETL concluído com sucesso")

    finally:
        cur_pipe.close(); conn_pipe.close()
        cur_op.close();   conn_op.close()
        cur_dw.close();   conn_dw.close()


if __name__ == "__main__":
    run_etl()
