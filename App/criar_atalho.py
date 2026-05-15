"""
Corre este script UMA VEZ para criar o atalho no ambiente de trabalho.
    python criar_atalho.py
"""
import os
import sys
import subprocess
from PIL import Image, ImageDraw

_dir        = os.path.dirname(os.path.abspath(__file__))
_start      = os.path.join(_dir, "start.py")
_icon       = os.path.join(_dir, "icon.ico")
_desktop    = os.path.join(os.path.expanduser("~"), "Desktop")
_atalho     = os.path.join(_desktop, "OP Report Manager.lnk")

# pythonw.exe não abre janela de consola ao fazer duplo clique
_pythonw = os.path.join(os.path.dirname(sys.executable), "pythonw.exe")
if not os.path.exists(_pythonw):
    _pythonw = sys.executable

# Gera e guarda o ícone como .ico
img  = Image.new("RGB", (64, 64), color=(26, 26, 24))
draw = ImageDraw.Draw(img)
draw.ellipse([6, 6, 58, 58], fill=(40, 81, 163))
draw.rectangle([20, 28, 44, 32], fill=(255, 255, 255))
draw.rectangle([20, 36, 36, 40], fill=(255, 255, 255))
draw.rectangle([20, 20, 28, 24], fill=(255, 255, 255))
img.save(_icon, format="ICO", sizes=[(64, 64), (32, 32), (16, 16)])

# Cria o atalho via PowerShell
ps = f"""
$s = (New-Object -ComObject WScript.Shell).CreateShortcut('{_atalho}')
$s.TargetPath       = '{_pythonw}'
$s.Arguments        = '"{_start}"'
$s.WorkingDirectory = '{_dir}'
$s.IconLocation     = '{_icon}'
$s.Description      = 'Inicia o OP Report Manager'
$s.Save()
"""
subprocess.run(["powershell", "-NoProfile", "-Command", ps], check=True)
print(f"Atalho criado: {_atalho}")
print("Podes fechar isto. O ícone está no ambiente de trabalho.")
