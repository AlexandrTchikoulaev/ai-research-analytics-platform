import psycopg2


def main():
    # -------------------------
    # Conexão
    # -------------------------
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="vector_db",
        user="projeto_utilizador",
        password="projeto"
    )
    cur = conn.cursor()

    # -------------------------
    # Ativar extensão pgvector
    # -------------------------
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    print("Extensão vector garantida")

    # -------------------------
    # Apagar tabela
    # -------------------------
    cur.execute("DROP TABLE IF EXISTS documents CASCADE;")
    print("Table documents dropped")

    # -------------------------
    # Criar tabela
    # -------------------------
    cur.execute("""
    CREATE TABLE documents (
        id SERIAL PRIMARY KEY,
        content TEXT,
        embedding VECTOR(1024),
        metadata JSONB
    );
    """)
    print("Table documents created")

    # -------------------------
    # Criar índice
    # -------------------------
    cur.execute("""
    CREATE INDEX idx_documents_embedding
    ON documents
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
    """)
    print("Index created")

    conn.commit()

    # -------------------------
    # Close Connection
    # -------------------------
    cur.close()
    conn.close()

    print("Setup completed")


# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    main()