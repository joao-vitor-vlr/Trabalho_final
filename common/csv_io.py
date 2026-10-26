"""Leitura do dataset (etapa DEPENDENTE / sequencial)."""

from __future__ import annotations

import csv
from pathlib import Path

from common.carga import Destinatario


def carregar_destinatarios(caminho: str | Path) -> list[Destinatario]:
    caminho = Path(caminho)
    if not caminho.exists():
        raise FileNotFoundError(
            f"{caminho} não encontrado. Gere com: python -m dataset.gerador_carga --registros 10000"
        )
    with caminho.open(newline="", encoding="utf-8") as f:
        return [
            Destinatario(
                id=int(row["id"]),
                nome=row["nome"],
                email=row["email"],
                segmento=row["segmento"],
                oferta_personalizada=row["oferta_personalizada"],
                opt_out=row["opt_out"].strip().lower() in ("true", "1", "sim"),
            )
            for row in csv.DictReader(f)
        ]
