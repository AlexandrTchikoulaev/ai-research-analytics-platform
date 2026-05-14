import subprocess
import sys
import time
import os
import threading
import urllib.request

from PIL import Image, ImageDraw
import pystray
import webview

_script_dir = os.path.dirname(os.path.abspath(__file__))
_api_process = None
_window = None

_python_exe = os.path.join(os.path.dirname(sys.executable), "python.exe")
_html_url = "file:///" + os.path.join(
    _script_dir, "Codes", "Website", "index.html"
).replace(os.sep, "/")


def _criar_imagem_icone():
    img = Image.new("RGB", (64, 64), color=(26, 26, 24))
    draw = ImageDraw.Draw(img)
    draw.ellipse([6, 6, 58, 58], fill=(40, 81, 163))
    draw.rectangle([20, 28, 44, 32], fill=(255, 255, 255))
    draw.rectangle([20, 36, 36, 40], fill=(255, 255, 255))
    draw.rectangle([20, 20, 28, 24], fill=(255, 255, 255))
    return img


def _on_closing():
    # Fechar a janela minimiza para a bandeja em vez de encerrar
    _window.hide()
    return False


def _mostrar_janela(icon=None, item=None):
    _window.show()


def _parar_sistema(icon, item):
    global _api_process
    if _api_process and _api_process.poll() is None:
        _api_process.terminate()
    for c in ["projeto_uc", "projeto_pgadmin", "minio"]:
        subprocess.run(f"docker stop {c}", shell=True,
                       creationflags=subprocess.CREATE_NO_WINDOW)
    icon.stop()
    _window.destroy()


def _esperar_docker(timeout=90):
    for _ in range(timeout):
        r = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        if r.returncode == 0:
            return True
        time.sleep(1)
    return False


def _iniciar():
    global _api_process

    # 1. Docker Desktop
    subprocess.Popen(
        r"C:\Program Files\Docker\Docker\Docker Desktop.exe",
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    _esperar_docker(timeout=90)
    time.sleep(2)

    # 2. Containers
    for c in ["projeto_uc", "projeto_pgadmin", "minio"]:
        subprocess.run(f"docker start {c}", shell=True,
                       creationflags=subprocess.CREATE_NO_WINDOW)
    time.sleep(5)

    # 3. MinIO buckets
    setup_path = os.path.join(_script_dir, "Codes", "Setup")
    subprocess.run(
        [sys.executable, "setup_minio_buckets.py"],
        cwd=setup_path,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )

    # 4. API
    _api_process = subprocess.Popen(
        [_python_exe, "-m", "uvicorn", "api:app", "--host", "localhost", "--port", "8000"],
        cwd=os.path.join(_script_dir, "Codes", "Website"),
        creationflags=subprocess.CREATE_NO_WINDOW,
    )

    # Aguarda a API estar pronta (até 60s)
    for _ in range(60):
        try:
            urllib.request.urlopen("http://localhost:8000/docs", timeout=1)
            break
        except Exception:
            time.sleep(1)

    # 5. Carrega a interface e exibe a janela nativa
    _window.load_url(_html_url)
    _window.show()
    _icon.notify("Sistema pronto!", "OP Report Manager")


def _setup_tray(icon):
    icon.visible = True
    threading.Thread(target=_iniciar, daemon=True).start()


_menu = pystray.Menu(
    pystray.MenuItem("Abrir", _mostrar_janela, default=True),
    pystray.Menu.SEPARATOR,
    pystray.MenuItem("Parar Sistema", _parar_sistema),
)

_icon = pystray.Icon(
    "op_report_manager",
    _criar_imagem_icone(),
    "OP Report Manager",
    _menu,
)

# pystray roda em thread de fundo — pywebview exige a main thread no Windows
threading.Thread(target=_icon.run, args=(_setup_tray,), daemon=True).start()

# Janela oculta até a API estar pronta; fechar minimiza para a bandeja
_window = webview.create_window(
    "OP Report Manager",
    "about:blank",
    width=1280,
    height=800,
    hidden=True,
)
_window.events.closing += _on_closing

webview.start()
