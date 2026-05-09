import subprocess
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

print("🛑 A desligar sistema...")

# 1. Parar API via PID
print("🌐 A parar API...")

if os.path.exists("api.pid"):
    with open("api.pid", "r") as f:
        pid = f.read().strip()
        subprocess.run(f"taskkill /F /PID {pid}", shell=True)
else:
    print("⚠️ PID não encontrado, a tentar método alternativo...")
    subprocess.run("taskkill /F /IM uvicorn.exe", shell=True)

# 2. Parar containers
print("📦 A desligar containers...")

containers = ["projeto_uc", "projeto_pgadmin", "minio"]

for c in containers:
    subprocess.run(f"docker stop {c}", shell=True)

print("✅ Sistema desligado!")