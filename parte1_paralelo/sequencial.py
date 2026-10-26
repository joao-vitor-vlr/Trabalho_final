"""Executor SEQUENCIAL — linha de base (1 processo, 1 thread).

    python -m parte1_paralelo.sequencial
    python -m parte1_paralelo.sequencial --profile
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parte1_paralelo.nucleo import main  # noqa: E402

if __name__ == "__main__":
    main("sequencial")
