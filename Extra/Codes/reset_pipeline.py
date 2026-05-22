"""
reset_pipeline.py
Limpa todos os dados da pipeline sem apagar a estrutura das tabelas.
  - warehouse_db  : TRUNCATE fact_values, dim_report, dim_indicator
  - gestao_db     : TRUNCATE op_data, op_report (CASCADE), etl_logs_dados, etl_logs_pdfs
  - vector_db     : DELETE langchain_pg_embedding e langchain_pg_collection
  - MinIO         : elimina todos os objetos dos buckets bronze/silver/bronze-unstructured/thumbnails
  - Reports       : elimina ficheiros em Reports/PDFs/ e Reports/Data/
"""
import os
import glob
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

BUCKETS = ["bronze", "silver", "bronze-unstructured", "thumbnails"]


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


def clear_vector_db():
    conn = _connect("vector_db")
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM langchain_pg_embedding;")
        emb = cur.rowcount
        cur.execute("DELETE FROM langchain_pg_collection;")
        col = cur.rowcount
        conn.commit()
        print(f"  OK  vector_db.langchain_pg_embedding — {emb} chunk(s) eliminado(s)")
        print(f"  OK  vector_db.langchain_pg_collection — {col} coleção(ões) eliminada(s)")
    except Exception as e:
        conn.rollback()
        print(f"  ERRO em vector_db: {e}")
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
        except Exception as e:
            print(f"  ERRO bucket '{bucket}': {e}")


def clear_reports():
    base = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "Reports"))
    patterns = [
        os.path.join(base, "PDFs", "pipeline_reports_report_*.txt"),
        os.path.join(base, "Data", "pipeline_data_report_*.txt"),
    ]
    total = 0
    for pattern in patterns:
        for path in glob.glob(pattern):
            try:
                os.remove(path)
                print(f"  OK  {os.path.relpath(path, base)}")
                total += 1
            except Exception as e:
                print(f"  ERRO ao apagar {path}: {e}")
    if total == 0:
        print("  Sem relatórios para apagar.")


if __name__ == "__main__":
    print("=" * 60)
    print("RESET PIPELINE")
    print("=" * 60)

    print("\n[1/5] Data Warehouse (warehouse_db)...")
    # dim_location e dim_date são seed estático (países ISO + anos) — não devem ser apagadas no reset
    truncate_tables("warehouse_db", [
        "fact_values",
        "dim_report",
        "dim_indicator",
    ])

    print("\n[2/5] Operational DB (gestao_db)...")
    truncate_tables("gestao_db", ["op_data", "op_report"])
    truncate_tables("gestao_db", ["etl_logs_dados", "etl_logs_pdfs"])

    print("\n[3/5] Vector DB (vector_db)...")
    clear_vector_db()

    print("\n[4/5] MinIO — buckets...")
    clear_minio()

    print("\n[5/5] Relatórios de pipeline...")
    clear_reports()

    print("\n" + "=" * 60)
    print("RESET CONCLUÍDO — sistema pronto para nova carga.")
    print("=" * 60)
