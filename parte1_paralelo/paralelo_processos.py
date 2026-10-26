"""Executor com PROCESSOS — concurrent.futures.ProcessPoolExecutor (2, 4 e 8 processos).

Cada processo tem o seu próprio interpretador e o seu próprio GIL, então tanto a
etapa CPU-bound quanto a I/O-bound escalam (até o nº de núcleos). O custo é o
spawn dos processos e a serialização (pickle) dos chunks/resultados — por isso
os destinatários são enviados em chunks e o resultado é um Counter pequeno.

    python -m parte1_paralelo.paralelo_processos               # 2, 4 e 8 processos
    python -m parte1_paralelo.paralelo_processos --workers 4 --chunks-por-worker 8
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parte1_paralelo.nucleo import main  # noqa: E402

if __name__ == "__main__":  # guarda obrigatória: macOS/Windows usam "spawn"
    main("processos")
