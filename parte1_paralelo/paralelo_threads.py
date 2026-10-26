"""Executor com THREADS — concurrent.futures.ThreadPoolExecutor (2, 4 e 8 threads).

Threads compartilham o mesmo interpretador e, portanto, o mesmo GIL:
  * etapa I/O-bound (sleep/socket)   -> libera o GIL  -> escala com o nº de threads
  * etapa CPU-bound (SHA-256 em loop) -> segura o GIL -> serializa (não escala)

    python -m parte1_paralelo.paralelo_threads                 # 2, 4 e 8 threads
    python -m parte1_paralelo.paralelo_threads --workers 16
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parte1_paralelo.nucleo import main  # noqa: E402

if __name__ == "__main__":
    main("threads")
