"""
reset_pipeline.py
Limpa todos os dados da pipeline sem apagar a estrutura das tabelas.
  - warehouse_db  : TRUNCATE todas as tabelas do DW
  - operational_db: TRUNCATE op_data e op_report (CASCADE)
  - pipeline_db   : TRUNCATE logs; reset etl_data.last_run -> NULL
  - MinIO         : elimina todos os objetos dos buckets bronze/silver
"""
import psycopg2
import boto3

_BASE = {
    "host": "localhost",
    "port": 5433,
    "user": "projeto_utilizador",
    "password": "projeto",
}

MINIO = {
    "endpoint_url": "http://localhost:9002",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123",
}

BUCKETS = ["bronze", "silver", "bronze-unstructured"]


def _connect(dbname: str):
    return psycopg2.connect(**_BASE, dbname=dbname)


def truncate_tables(dbname: str, tables: list[str]):
    conn = _connect(dbname)
    cur = conn.cursor()
    try:
        joined = ", ".join(f'"{t}"' for t in tables)
        cur.execute(f"TRUNCATE TABLE {joined} RESTART IDENTITY CASCADE;")
        conn.commit()
        for t in tables:
            print(f"  OK  {dbname}.{t}")
    except Exception as e:
        conn.rollback()
        print(f"  ERRO em {dbname}: {e}")
    finally:
        cur.close()
        conn.close()


def reset_etl_timestamps():
    conn = _connect("gestao_db")
    cur = conn.cursor()
    try:
        cur.execute("UPDATE etl_data SET last_run = NULL;")
        rows = cur.rowcount
        conn.commit()
        print(f"  OK  pipeline_db.etl_data — {rows} processo(s) resetado(s)")
    except Exception as e:
        conn.rollback()
        print(f"  ERRO: {e}")
    finally:
        cur.close()
        conn.close()


def clear_minio():
    s3 = boto3.client("s3", **MINIO)
    for bucket in BUCKETS:
        try:
            pag = s3.get_paginator("list_objects_v2")
            deleted = 0
            for page in pag.paginate(Bucket=bucket):
                objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if objs:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
                    deleted += len(objs)
            print(f"  OK  bucket '{bucket}': {deleted} objeto(s) eliminado(s)")
        except s3.exceptions.NoSuchBucket:
            print(f"  SKIP bucket '{bucket}': não existe")
        except Exception as e:
            print(f"  ERRO bucket '{bucket}': {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("RESET PIPELINE")
    print("=" * 60)

    print("\n[1/4] Data Warehouse (warehouse_db)...")
    # dim_location e dim_date são seed estático (países ISO + anos) — não devem ser apagadas no reset
    truncate_tables("warehouse_db", [
        "fact_values",
        "dim_report",
        "dim_indicator",
    ])

    print("\n[2/4] Operational DB (gestao_db)...")
    # op_data tem FK para op_report — CASCADE trata isso automaticamente
    truncate_tables("gestao_db", ["op_data", "op_report"])

    print("\n[3/4] Pipeline DB — logs e timestamps (gestao_db)...")
    truncate_tables("gestao_db", ["etl_logs_dados", "etl_logs_pdfs"])
    reset_etl_timestamps()

    print("\n[4/4] MinIO — buckets Bronze e Silver...")
    clear_minio()

    print("\n" + "=" * 60)
    print("RESET CONCLUÍDO — sistema pronto para nova carga.")
    print("=" * 60)
