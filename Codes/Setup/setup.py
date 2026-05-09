import subprocess
import sys
import time
import os

# ===============================
# CONFIGURAÇÕES
# ===============================

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

DB_VECTOR = {
    "host": "localhost",
    "port": 5433,
    "dbname": "vector_db",
    "user": "projeto_utilizador",
    "password": "projeto",
}

MINIO_CONFIG = {
    "endpoint": "localhost:9002",
    "access_key": "admin",
    "secret_key": "admin123",
    "secure": False,
}

MINIO_BUCKETS = ["bronze", "silver", "bronze-unstructured"]

# Scripts de criação (ordem importa)
CREATE_SCRIPTS = [
    "Codes/Setup/setup_etl_data.py",
    "Codes/Setup/setup_etl_logs.py",
    "Codes/Setup/setup_opdb.py",
    "Codes/Setup/setup_datawarehouse.py",
    "Codes/Setup/setup_vectorial_db.py",
]

POPULATE_SCRIPT = "Extra/populate_opdb_csv.py"

ETL_SCRIPT = "Codes/Pipeline/pipeline_dados.py"


# ===============================
# HELPERS DE OUTPUT
# ===============================

def step(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")

def ok(msg):
    print(f"  {msg}")

def warn(msg):
    print(f"  {msg}")

def err(msg):
    print(f"  {msg}")

def info(msg):
    print(f"  {msg}")


# ===============================
# VERIFICAR DEPENDÊNCIAS
# ===============================

def check_dependencies():
    step("A verificar dependências do sistema")

    # Verificar Docker
    result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
    if result.returncode != 0:
        err("Docker não encontrado. Instala o Docker e tenta novamente.")
        sys.exit(1)
    ok(f"Docker: {result.stdout.strip()}")

    # Verificar Docker Compose
    result = subprocess.run(["docker", "compose", "version"], capture_output=True, text=True)
    if result.returncode != 0:
        # tentar versão antiga
        result = subprocess.run(["docker-compose", "--version"], capture_output=True, text=True)
        if result.returncode != 0:
            err("Docker Compose não encontrado.")
            sys.exit(1)
    ok(f"Docker Compose: {result.stdout.strip()}")

    # Verificar ficheiro docker-compose.yml
    if not os.path.exists("Docker/docker-compose.yml"):
        err("Ficheiro Docker/docker-compose.yml não encontrado.")
        sys.exit(1)
    ok("Docker/docker-compose.yml encontrado")

    # Verificar dependências Python
    required_packages = {
        "psycopg2": "psycopg2-binary",
        "boto3": "boto3",
        "minio": "minio",
        "requests": "requests",
        "pandas": "pandas",
        "pyarrow": "pyarrow",
    }

    missing = []
    for pkg, install_name in required_packages.items():
        try:
            __import__(pkg)
            ok(f"Python package '{pkg}' disponível")
        except ImportError:
            warn(f"Python package '{pkg}' em falta — será instalado")
            missing.append(install_name)

    if missing:
        info(f"A instalar: {', '.join(missing)}")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install"] + missing,
            capture_output=True, text=True
        )
        if result.returncode != 0:
            err(f"Erro ao instalar dependências:\n{result.stderr}")
            sys.exit(1)
        ok("Dependências instaladas com sucesso")


# ===============================
# DOCKER COMPOSE
# ===============================

def start_docker():
    step("A iniciar serviços Docker")

    # Determinar comando correto
    compose_cmd = _get_compose_cmd()

    info("A executar docker compose up -d ...")
    result = subprocess.run(
        compose_cmd + ["up", "-d"],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        err(f"Erro ao iniciar Docker:\n{result.stderr}")
        sys.exit(1)

    ok("Serviços Docker iniciados")
    print(result.stdout)


def _get_compose_cmd():
    result = subprocess.run(["docker", "compose", "version"], capture_output=True)
    if result.returncode == 0:
        return ["docker", "compose", "-f", "Docker/docker-compose.yml"]
    return ["docker-compose", "-f", "Docker/docker-compose.yml"]


# ===============================
# AGUARDAR POSTGRES
# ===============================

def wait_for_postgres(retries=30, delay=3):
    step("A aguardar PostgreSQL ficar disponível")

    try:
        import psycopg2
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "psycopg2-binary"], check=True)
        import psycopg2

    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_WAREHOUSE)
            conn.close()
            ok(f"PostgreSQL disponível (tentativa {attempt}/{retries})")
            return
        except Exception as e:
            info(f"Tentativa {attempt}/{retries} — ainda não disponível ({e})")
            time.sleep(delay)

    err(f"PostgreSQL não ficou disponível após {retries} tentativas.")
    sys.exit(1)


# ===============================
# AGUARDAR MINIO
# ===============================

def wait_for_minio(retries=20, delay=3):
    step("A aguardar MinIO ficar disponível")

    try:
        import requests
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "requests"], check=True)
        import requests

    url = f"http://{MINIO_CONFIG['endpoint']}/minio/health/live"

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                ok(f"MinIO disponível (tentativa {attempt}/{retries})")
                return
        except Exception as e:
            info(f"Tentativa {attempt}/{retries} — ainda não disponível ({e})")
        time.sleep(delay)

    err(f"MinIO não ficou disponível após {retries} tentativas.")
    sys.exit(1)


# ===============================
# CRIAR BUCKETS MINIO
# ===============================

def create_minio_buckets():
    step("A criar buckets MinIO")

    try:
        from minio import Minio
        from minio.error import S3Error
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "minio"], check=True)
        from minio import Minio
        from minio.error import S3Error

    client = Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=MINIO_CONFIG["secure"],
    )

    for bucket in MINIO_BUCKETS:
        try:
            if client.bucket_exists(bucket):
                warn(f"Bucket '{bucket}' já existe — ignorado")
            else:
                client.make_bucket(bucket)
                ok(f"Bucket '{bucket}' criado")
        except S3Error as e:
            err(f"Erro ao criar bucket '{bucket}': {e}")
            sys.exit(1)


# ===============================
# CRIAR TABELAS (SCRIPTS CREATE)
# ===============================

def create_database_tables():
    step("A criar tabelas na base de dados")

    for script in CREATE_SCRIPTS:
        if not os.path.exists(script):
            warn(f"Script não encontrado: {script} — a ignorar")
            continue

        info(f"A executar: {script}")
        result = subprocess.run(
            [sys.executable, script],
            capture_output=True, text=True
        )

        if result.returncode != 0:
            err(f"Erro em {script}:\n{result.stderr}")
            sys.exit(1)

        ok(f"{script} concluído")
        if result.stdout.strip():
            for line in result.stdout.strip().splitlines():
                print(f"     {line}")


# ===============================
# POPULAR BASE OPERACIONAL
# ===============================

def populate_operational_db():
    step("A popular base de dados operacional (CSV)")

    # Verificar se os CSVs existem
    csv_report = os.path.join("Extra", "csv", "op_report.csv")
    csv_data   = os.path.join("Extra", "csv", "op_data.csv")

    if not os.path.exists(csv_report) or not os.path.exists(csv_data):
        warn("Ficheiros CSV não encontrados em Extra/csv/op_report.csv e Extra/csv/op_data.csv")
        warn("A ignorar o passo de populamento. Podes executar Extra/populate_opdb_csv.py manualmente mais tarde.")
        return

    if not os.path.exists(POPULATE_SCRIPT):
        warn(f"Script {POPULATE_SCRIPT} não encontrado — a ignorar")
        return

    info(f"A executar: {POPULATE_SCRIPT}")
    result = subprocess.run(
        [sys.executable, POPULATE_SCRIPT],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        err(f"Erro em {POPULATE_SCRIPT}:\n{result.stderr}")
        sys.exit(1)

    ok(f"{POPULATE_SCRIPT} concluído")
    if result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            print(f"     {line}")

# ===============================
# CORRER ETL
# ===============================

def execute_etl():
    step("A executar etl")

    if not os.path.exists(ETL_SCRIPT):
        warn(f"Script não encontrado: {ETL_SCRIPT} — a ignorar")
        return

    info(f"A executar: {ETL_SCRIPT}\n")
    
    # Ao retirar o capture_output, o output vai direto para o teu terminal em tempo real
    result = subprocess.run(
        [sys.executable, ETL_SCRIPT]
    )

    if result.returncode != 0:
        err(f"Erro ao executar {ETL_SCRIPT}! (Verifica o erro detalhado acima nas linhas do terminal)")
        sys.exit(1)

    ok(f"{ETL_SCRIPT} concluído")

# ===============================
# VERIFICAÇÃO FINAL
# ===============================

def verify_setup():
    step("A verificar setup final")

    import psycopg2

    todas_ok = True

    checks = [
        (DB_WAREHOUSE,    "warehouse_db",    ["dim_indicator", "dim_location", "dim_location_hierarchy", "dim_date", "dim_report", "fact_values"]),
        (DB_OPERATIONAL,  "operational_db",  ["op_report", "op_data"]),
        (DB_PIPELINE,     "pipeline_db",     ["etl_data", "etl_logs"]),
        (DB_VECTOR,       "vector_db",       ["documents"]),
    ]

    for config, db_name, tabelas_esperadas in checks:
        try:
            conn = psycopg2.connect(**config)
            cur = conn.cursor()
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            existentes = {row[0] for row in cur.fetchall()}
            for tabela in tabelas_esperadas:
                if tabela in existentes:
                    ok(f"[{db_name}] '{tabela}' existe")
                else:
                    warn(f"[{db_name}] '{tabela}' NÃO encontrada")
                    todas_ok = False
            cur.close()
            conn.close()
        except Exception as e:
            err(f"Erro a verificar {db_name}: {e}")
            todas_ok = False

    # Verificar last_run em etl_data (pipeline_db)
    try:
        conn = psycopg2.connect(**DB_PIPELINE)
        cur = conn.cursor()
        cur.execute("SELECT last_run FROM etl_data WHERE process_name = 'etl_main'")
        row = cur.fetchone()
        if row:
            ok(f"etl_data.last_run = {row[0]}")
        else:
            warn("Registo etl_main não encontrado em etl_data")
            todas_ok = False
        cur.close()
        conn.close()
    except Exception as e:
        err(f"Erro ao verificar etl_data: {e}")
        todas_ok = False

    # Verificar populamento das tabelas operacionais
    try:
        conn = psycopg2.connect(**DB_OPERATIONAL)
        cur = conn.cursor()
        for tabela in ["op_report", "op_data"]:
            cur.execute(f"SELECT COUNT(*) FROM {tabela}")
            count = cur.fetchone()[0]
            if count > 0:
                ok(f"[operational_db] '{tabela}' populada ({count} registos)")
            else:
                warn(f"[operational_db] '{tabela}' está vazia — verifica os CSVs em populate/csv/")
                todas_ok = False
        cur.close()
        conn.close()
    except Exception as e:
        err(f"Erro ao verificar operational_db: {e}")
        todas_ok = False

    return todas_ok


# ===============================
# SUMÁRIO FINAL
# ===============================

def print_summary():
    print(f"\n{'='*60}")
    print("  SETUP CONCLUÍDO COM SUCESSO")
    print(f"{'='*60}")
    print()
    print("  Serviços disponíveis:")
    print(f"  • warehouse_db    → localhost:5433/warehouse_db")
    print(f"  • operational_db  → localhost:5433/operational_db")
    print(f"  • pipeline_db     → localhost:5433/pipeline_db")
    print(f"  • vector_db       → localhost:5433/vector_db")
    print(f"  • MinIO API   → http://localhost:9002")
    print(f"  • MinIO UI    → http://localhost:9003")
    print(f"  • pgAdmin     → http://localhost:5051")
    print()
    print("  Próximos passos:")
    print("     (Opcional) Ingestão vetorial:")
    print("     python "Codes/Pipeline Unstructured/ingest_vectorialdb.py"")
    print(f"\n{'='*60}\n")


# ===============================
# MAIN
# ===============================

def main():
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(os.path.dirname(os.path.dirname(_script_dir)))

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║         SETUP — Pipeline de Dados                        ║")
    print("╚══════════════════════════════════════════════════════════╝")

    check_dependencies()
    start_docker()
    wait_for_postgres()
    wait_for_minio()
    create_minio_buckets()
    create_database_tables()
    populate_operational_db()
    execute_etl()
    all_ok = verify_setup()

    if all_ok:
        print_summary()
    else:
        print()
        warn("Setup concluído com alguns avisos. Verifica as mensagens acima.")
        print()


if __name__ == "__main__":
    main()