"""
Testa o gerador de funções silver com um ficheiro JSON do IMF.

Uso:
    python test_imf_generator.py imf_example.json
    python test_imf_generator.py <caminho/para/ficheiro.json>
"""

import sys
import os

# Adiciona o diretório Pipeline ao path
_pipeline_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Codes", "Pipeline")
sys.path.insert(0, _pipeline_dir)

from silver_function_generator import generate_and_validate


def _resolve_path(arg: str) -> str:
    if os.path.isabs(arg):
        return arg
    # Tenta relativo ao diretório do script (Extra/)
    local = os.path.join(os.path.dirname(os.path.abspath(__file__)), arg)
    if os.path.exists(local):
        return local
    # Tenta relativo ao cwd
    return os.path.abspath(arg)


def main():
    if len(sys.argv) < 2:
        print("Uso: python test_imf_generator.py <ficheiro.json>")
        sys.exit(1)

    path = _resolve_path(sys.argv[1])

    if not os.path.exists(path):
        print(f"Ficheiro não encontrado: {path}")
        sys.exit(1)

    print(f"Ficheiro  : {path}")
    print(f"A gerar função...\n")

    with open(path, "rb") as f:
        content = f.read()

    result = generate_and_validate(content)

    print(f"Formato   : {result['fmt']}")
    print(f"Função    : {result['function_name']}")
    print(f"Gerada    : {result['generated']}")
    print(f"Válida    : {result['valid']}")

    if result["error"]:
        print(f"\nErro      : {result['error']}")

    if result["preview"]:
        print(f"\nPreview (3 linhas):")
        for row in result["preview"]:
            print(f"  {row}")

    if result["code"]:
        print(f"\n{'=' * 60}")
        print("Código gerado:")
        print('=' * 60)
        print(result["code"])


if __name__ == "__main__":
    main()
