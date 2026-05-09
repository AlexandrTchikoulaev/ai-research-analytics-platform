import subprocess
import sys
import time
import webbrowser
import os

_script_dir = os.path.dirname(os.path.abspath(__file__))

print("🚀 A iniciar o sistema...")

# 1. Abrir Docker Desktop
print("🐳 A abrir Docker Desktop...")
subprocess.Popen(r"C:\Program Files\Docker\Docker\Docker Desktop.exe")

# 2. Esperar Docker arrancar
print("⏳ A aguardar Docker iniciar...")
time.sleep(15)

# 3. Ligar containers
containers = ["projeto_uc", "projeto_pgadmin", "minio"]

print("📦 A ligar containers do projeto...")

for c in containers:
    subprocess.run(f"docker start {c}", shell=True)

# 4. Esperar DB + MinIO
time.sleep(5)

# 5. Criar buckets MinIO
print("🪣 A criar buckets MinIO...")
setup_path = os.path.join(_script_dir, "Codes", "Setup")
subprocess.run([sys.executable, "setup_minio_buckets.py"], cwd=setup_path)

# 6. API
print("🌐 A iniciar API...")
api_process = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "api:app", "--reload"],
    cwd=os.path.join(_script_dir, "Codes", "Website")
)

# 7. Frontend
print("🖥️ A abrir website...")
html_path = os.path.join(_script_dir, "Codes", "Website", "index.html")
webbrowser.open(f"file://{html_path}")

print("✅ Sistema iniciado!")

try:
    api_process.wait()
except KeyboardInterrupt:
    print("\n🛑 A desligar sistema...")
    api_process.terminate()
