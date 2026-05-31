import psycopg2
import psycopg2.extras
import pandas as pd
import boto3
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import DB_CONFIG, DB_WAREHOUSE, MINIO_CONFIG, BUCKET_SILVER

MAX_GOLD_WORKERS = 4


def read_parquet_from_s3(s3, key):
    response = s3.get_object(Bucket=BUCKET_SILVER, Key=key)
    return pd.read_parquet(BytesIO(response["Body"].read()), engine="pyarrow")


def get_mapping(cur_op):
    """Retorna {str(file_id): report_id} para ficheiros SILVER."""
    cur_op.execute("SELECT file_id, report_id FROM op_data WHERE pipeline_status = 'silver'")
    return {str(r[0]): r[1] for r in cur_op.fetchall()}


def load_source_and_reports(cur_op, cur_dw, conn_dw):
    cur_op.execute("""
        SELECT DISTINCT r.report_id, r.source_code, r.report_url, r.publication_date
        FROM op_report r
        JOIN op_data d ON d.report_id = r.report_id
        WHERE d.pipeline_status IN ('silver', 'done')
    """)
    for report_id, source_code, report_url, publication_date in cur_op.fetchall():
        cur_dw.execute("""
            INSERT INTO dim_report (
                report_id, source_code, source_name, report_url, publication_date
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (report_id) DO NOTHING
        """, (report_id, source_code, source_code, report_url, publication_date))
    conn_dw.commit()
    print("Source + Reports carregados")


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


def _load_one(file_id, df, report_id, source_system, loc_map, ind_sub, date_map):
    """Insere facts e atualiza status — cada worker usa ligações próprias."""
    conn_op = psycopg2.connect(**DB_CONFIG)
    conn_dw = psycopg2.connect(**DB_WAREHOUSE)
    cur_op  = conn_op.cursor()
    cur_dw  = conn_dw.cursor()

    def _fail(msg):
        try:
            cur_op.execute(
                "UPDATE op_data SET pipeline_status = 'failed' WHERE file_id = %s",
                (file_id,),
            )
            cur_op.execute(
                "INSERT INTO etl_logs_dados (file_id, step, error_message) VALUES (%s, %s, %s)",
                (file_id, "gold", msg),
            )
            conn_op.commit()
        except Exception:
            pass

    try:
        ind_sub_file = ind_sub.get(source_system, {})

        df = df.copy()
        df["location_sk"]  = df["location_code"].map(loc_map)
        df["indicator_sk"] = df["indicator_code"].map(ind_sub_file)
        df["date_id"]      = df["year"].map(date_map)

        valid_df = df[
            df["location_sk"].notna() &
            df["indicator_sk"].notna() &
            df["date_id"].notna()
        ].copy()

        if valid_df.empty:
            error_msg = (
                f"0 linhas válidas de {len(df)} — "
                f"location_sk em falta: {int(df['location_sk'].isna().sum())}, "
                f"indicator_sk em falta: {int(df['indicator_sk'].isna().sum())}, "
                f"date_id em falta: {int(df['date_id'].isna().sum())}"
            )
            _fail(error_msg)
            return False, error_msg

        valid_df["report_id"]    = report_id
        valid_df["location_sk"]  = valid_df["location_sk"].astype(int)
        valid_df["indicator_sk"] = valid_df["indicator_sk"].astype(int)
        valid_df["date_id"]      = valid_df["date_id"].astype(int)

        insert_data = valid_df[
            ["report_id", "location_sk", "indicator_sk", "date_id", "value"]
        ].values.tolist()

        psycopg2.extras.execute_values(cur_dw, """
            INSERT INTO fact_values (
                report_id, location_sk, indicator_sk, date_id, value
            )
            VALUES %s
            ON CONFLICT DO NOTHING
        """, insert_data, page_size=10000)
        conn_dw.commit()

        cur_op.execute(
            "UPDATE op_data SET pipeline_status = 'done' "
            "WHERE file_id = %s AND pipeline_status = 'silver'",
            (file_id,),
        )
        conn_op.commit()
        print(f"  [OK]   {file_id}.parquet")
        return True, None

    except Exception as e:
        _fail(str(e))
        print(f"  [ERRO] {file_id}: {e}")
        return False, str(e)

    finally:
        cur_op.close(); conn_op.close()
        cur_dw.close(); conn_dw.close()


def run_etl():
    s3 = boto3.client("s3", **MINIO_CONFIG)

    conn_op = psycopg2.connect(**DB_CONFIG)
    cur_op  = conn_op.cursor()
    conn_dw = psycopg2.connect(**DB_WAREHOUSE)
    cur_dw  = conn_dw.cursor()

    def _fail_main(file_id, msg):
        cur_op.execute(
            "UPDATE op_data SET pipeline_status = 'failed' WHERE file_id = %s",
            (file_id,),
        )
        cur_op.execute(
            "INSERT INTO etl_logs_dados (file_id, step, error_message) VALUES (%s, %s, %s)",
            (file_id, "gold", msg),
        )
        conn_op.commit()

    try:
        report_map = get_mapping(cur_op)

        if not report_map:
            print("Sem novos dados para carregar.")
            ensure_views(cur_dw, conn_dw)
            return

        print("1. Source + Reports")
        load_source_and_reports(cur_op, cur_dw, conn_dw)

        # Dimensões estáticas — carregadas uma única vez
        cur_dw.execute("SELECT location_code, location_sk FROM dim_location")
        loc_map = {code: sk for code, sk in cur_dw.fetchall()}

        cur_dw.execute("SELECT year, date_id FROM dim_date")
        date_map = {year: date_id for year, date_id in cur_dw.fetchall()}

        cur_op.execute("SELECT report_id, source_code FROM op_report")
        report_source_map = {r[0]: r[1] for r in cur_op.fetchall()}

        # Fase 1: ler todos os parquets e recolher pares de indicadores
        print("2. A ler parquets e a recolher indicadores...")
        file_data = {}  # {file_id: (df, report_id, source_system)}
        all_ind_pairs: dict[str, set] = {}  # {source_system: {(code, name)}}

        for nome_base, report_id in report_map.items():
            file_id       = int(nome_base)
            ficheiro      = nome_base + ".parquet"
            source_system = report_source_map.get(report_id)

            if not source_system:
                _fail_main(file_id, f"source_system não encontrado para report_id {report_id}")
                continue

            try:
                df = read_parquet_from_s3(s3, ficheiro)
            except Exception as e:
                _fail_main(file_id, f"Erro ao ler parquet: {e}")
                continue

            required = {"location_code", "indicator_code", "indicator_name", "year", "value"}

            if df.empty:
                _fail_main(file_id, "Parquet gerado está vazio")
                continue

            missing_cols = sorted(required - set(df.columns))
            if missing_cols:
                _fail_main(file_id, f"Colunas em falta no parquet: {missing_cols}")
                continue

            file_data[file_id] = (df, report_id, source_system)

            pairs = set(map(tuple, df[["indicator_code", "indicator_name"]].drop_duplicates().values.tolist()))
            all_ind_pairs.setdefault(source_system, set()).update(pairs)

        if not file_data:
            print("Sem ficheiros válidos para carregar.")
            ensure_views(cur_dw, conn_dw)
            return

        # Fase 2: inserir TODOS os indicadores de uma vez
        print("3. Dimensões — a inserir indicadores em lote...")
        all_ind_rows = [
            (source_system, code, name)
            for source_system, pairs in all_ind_pairs.items()
            for code, name in pairs
        ]
        if all_ind_rows:
            psycopg2.extras.execute_values(cur_dw, """
                INSERT INTO dim_indicator (source_system, indicator_code, indicator_name)
                VALUES %s
                ON CONFLICT DO NOTHING
            """, all_ind_rows, page_size=5000)
            conn_dw.commit()

        # Fase 3: construir ind_sub global (uma query, todos os source_systems)
        source_systems = list(all_ind_pairs.keys())
        ind_sub: dict[str, dict] = {}
        if source_systems:
            cur_dw.execute(
                "SELECT source_system, indicator_code, indicator_sk FROM dim_indicator WHERE source_system = ANY(%s)",
                (source_systems,),
            )
            for ss, code, sk in cur_dw.fetchall():
                ind_sub.setdefault(ss, {})[code] = sk

        # Fase 4: inserção paralela de facts
        print(f"4. Facts — a inserir em paralelo ({MAX_GOLD_WORKERS} workers)...")
        ok_count = err_count = 0

        with ThreadPoolExecutor(max_workers=MAX_GOLD_WORKERS) as executor:
            futures = {
                executor.submit(_load_one, file_id, df, report_id, source_system, loc_map, ind_sub, date_map): file_id
                for file_id, (df, report_id, source_system) in file_data.items()
            }
            for future in as_completed(futures):
                try:
                    success, _ = future.result()
                except Exception as e:
                    fid = futures[future]
                    print(f"  [ERRO] file_id={fid} exceção não tratada: {e}")
                    success = False
                if success:
                    ok_count += 1
                else:
                    err_count += 1

        print("5. Views")
        ensure_views(cur_dw, conn_dw)
        print(f"ETL concluído — {ok_count} OK, {err_count} erros")

    finally:
        cur_op.close(); conn_op.close()
        cur_dw.close(); conn_dw.close()


if __name__ == "__main__":
    run_etl()
