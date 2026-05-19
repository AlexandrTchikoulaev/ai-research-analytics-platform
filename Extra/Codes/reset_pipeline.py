"""
reset_pipeline.py
Limpa todos os dados da pipeline sem apagar a estrutura das tabelas.
  - warehouse_db  : TRUNCATE fact_values, dim_report, dim_indicator
  - gestao_db     : TRUNCATE op_data, op_report (CASCADE), etl_logs_dados, etl_logs_pdfs
  - vector_db     : DELETE langchain_pg_embedding e langchain_pg_collection
  - MinIO         : elimina todos os objetos dos buckets bronze/silver/bronze-unstructured/thumbnails
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
    truncate_tables("gestao_db", ["op_data", "op_report"])
    truncate_tables("gestao_db", ["etl_logs_dados", "etl_logs_pdfs"])

    print("\n[3/4] Vector DB (vector_db)...")
    clear_vector_db()

    print("\n[4/4] MinIO — buckets...")
    clear_minio()

    print("\n" + "=" * 60)
    print("RESET CONCLUÍDO — sistema pronto para nova carga.")
    print("=" * 60)
