"""Gerador de carga padronizada: ``destinatarios.csv``.

Uso:
    python -m dataset.gerador_carga --registros 10000
    python -m dataset.gerador_carga --registros 50000 --seed 7 --saida dataset/destinatarios.csv

Colunas: id, nome, email, segmento, oferta_personalizada, opt_out
  * ~2% com opt_out=True
  * ~3% com e-mails sintaticamente inválidos (para exercitar a validação)
  * e-mails válidos são ÚNICOS (o id compõe o endereço), requisito para que a
    chave de idempotência sha256(campanha:email) identifique um único cliente.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
import unicodedata
from collections import Counter
from pathlib import Path

if __package__ in (None, ""):  # permite "python dataset/gerador_carga.py"
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.carga import CAMPOS_CSV, email_valido  # noqa: E402

PRIMEIROS = [
    "Ana", "Bruno", "Carla", "Diego", "Eduarda", "Felipe", "Gabriela", "Henrique", "Isabela",
    "João", "Larissa", "Marcos", "Natália", "Otávio", "Paula", "Rafael", "Sofia", "Thiago",
    "Úrsula", "Vinícius", "Yasmin", "Lucas", "Mariana", "Pedro", "Beatriz", "Gustavo",
    "Letícia", "Rodrigo", "Camila", "André", "Fernanda", "Caio", "Juliana", "Mateus",
]
SOBRENOMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves", "Pereira",
    "Lima", "Gomes", "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida", "Lopes",
    "Soares", "Fernandes", "Vieira", "Barbosa", "Rocha", "Dias", "Nascimento", "Araújo",
]
DOMINIOS = ["gmail.com", "outlook.com", "yahoo.com.br", "uol.com.br", "empresa.com.br",
            "hotmail.com", "icloud.com", "bol.com.br"]
SEGMENTOS = {
    "varejo": ["20% OFF em toda a loja", "Frete grátis acima de R$ 99", "Leve 3 pague 2"],
    "tecnologia": ["Notebook com 15% OFF", "Fone Bluetooth por R$ 129", "Upgrade de SSD grátis"],
    "saude": ["Check-up completo com 30% OFF", "Plano odontológico sem carência"],
    "educacao": ["Curso de Python com 40% OFF", "1º mês grátis na pós-graduação"],
    "financeiro": ["Cartão sem anuidade", "Cashback de 5% no 1º mês", "Taxa zero no Pix"],
    "viagens": ["Pacote Nordeste em 10x sem juros", "Hotel com café incluso -25%"],
}

# Cada gerador produz um endereço que a Regex DEVE rejeitar.
INVALIDOS = [
    lambda u, d: f"{u}{d}",                  # sem @
    lambda u, d: f"{u}@@{d}",                # @ duplicado
    lambda u, d: f"{u}@",                    # sem domínio
    lambda u, d: f"@{u}.{d}",                # sem parte local (começa com @)
    lambda u, d: f"{u}@{d.split('.')[0]}",   # sem TLD
    lambda u, d: f"{u.replace('.', ' ')}@{d}",  # espaço
    lambda u, d: f"{u}..x@{d}",              # pontos consecutivos
    lambda u, d: f"{u}@{d}.c",               # TLD de 1 letra
    lambda u, d: f"{u}@exa_mple.com",        # underscore no domínio
    lambda u, d: f".{u}@{d}",                # começa com ponto
]


def ascii_slug(texto: str) -> str:
    sem_acento = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    return sem_acento.lower()


def gerar(registros: int, seed: int, taxa_opt_out: float, taxa_invalidos: float):
    rng = random.Random(seed)
    segmentos = list(SEGMENTOS)
    for i in range(1, registros + 1):
        primeiro, sobrenome = rng.choice(PRIMEIROS), rng.choice(SOBRENOMES)
        nome = f"{primeiro} {sobrenome}"
        usuario = f"{ascii_slug(primeiro)}.{ascii_slug(sobrenome)}{i}"
        dominio = rng.choice(DOMINIOS)
        if rng.random() < taxa_invalidos:
            email = rng.choice(INVALIDOS)(usuario, dominio)
        else:
            email = f"{usuario}@{dominio}"
        segmento = rng.choice(segmentos)
        yield {
            "id": i,
            "nome": nome,
            "email": email,
            "segmento": segmento,
            "oferta_personalizada": rng.choice(SEGMENTOS[segmento]),
            "opt_out": rng.random() < taxa_opt_out,
        }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--registros", type=int, default=10_000, help="10.000 a 50.000 (padrão: 10000)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--saida", default=str(Path(__file__).resolve().parent / "destinatarios.csv"))
    p.add_argument("--taxa-opt-out", type=float, default=0.02)
    p.add_argument("--taxa-invalidos", type=float, default=0.03)
    p.add_argument("--permitir-fora-faixa", action="store_true",
                   help="permite < 10.000 ou > 50.000 registros (útil para smoke tests)")
    a = p.parse_args(argv)

    if not (10_000 <= a.registros <= 50_000) and not a.permitir_fora_faixa:
        p.error("--registros deve estar entre 10000 e 50000 (ou use --permitir-fora-faixa)")

    saida = Path(a.saida)
    saida.parent.mkdir(parents=True, exist_ok=True)
    stats = Counter()
    with saida.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_CSV)
        w.writeheader()
        for row in gerar(a.registros, a.seed, a.taxa_opt_out, a.taxa_invalidos):
            w.writerow(row)
            stats["total"] += 1
            stats["opt_out"] += row["opt_out"]
            stats["invalidos"] += not email_valido(row["email"])

    t = stats["total"]
    print(f"[gerador] {t} registros -> {saida}")
    print(f"[gerador] opt_out={stats['opt_out']} ({stats['opt_out']/t:.2%}) | "
          f"invalidos={stats['invalidos']} ({stats['invalidos']/t:.2%}) | seed={a.seed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
