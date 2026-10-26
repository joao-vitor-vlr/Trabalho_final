"""Topologia do broker (RabbitMQ) e chaves do Redis compartilhadas por API e Workers.

Declarar a fila com argumentos IDÊNTICOS nos dois lados é obrigatório no
RabbitMQ (senão ocorre PRECONDITION_FAILED), por isso a definição vive aqui.
"""

from __future__ import annotations

import os

FILA_LOTES = os.getenv("FILA_LOTES", "emails.lotes")
FILA_DLQ = f"{FILA_LOTES}.dlq"  # mensagens venenosas (JSON inválido etc.)

ESTADOS_TERMINAIS = ("ENVIADO", "ERRO_SMTP", "INVALIDO", "OPT_OUT")
CATEGORIAS = ("enviados", "invalidos", "opt_out", "erros_smtp")


def k_meta(cid: str) -> str:
    return f"campanha:{cid}:meta"


def k_template(cid: str) -> str:
    return f"campanha:{cid}:template"


def k_stats(cid: str) -> str:
    return f"campanha:{cid}:stats"


def k_idem(idem_key: str) -> str:
    return f"idem:{idem_key}"


async def declarar_filas(canal):
    """Declara a fila durável de lotes (com DLQ). Retorna o objeto da fila principal."""
    await canal.declare_queue(FILA_DLQ, durable=True)
    return await canal.declare_queue(
        FILA_LOTES,
        durable=True,  # a fila sobrevive a restart do broker
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": FILA_DLQ,
        },
    )
