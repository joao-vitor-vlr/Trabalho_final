"""Nó 4 — Servidor SMTP Mock (FastAPI).

Simula um provedor de e-mail transacional acessado via HTTP:
  * latência controlada (padrão 5–15 ms) por requisição;
  * falha PERMANENTE determinística (~0,5%, "550 mailbox unavailable") -> HTTP 422;
  * falha TRANSITÓRIA aleatória (padrão 2%, "451 try again")          -> HTTP 503;
  * suporte ao cabeçalho ``Idempotency-Key`` (como APIs reais de provedores):
    uma segunda entrega com a mesma chave NÃO é entregue de novo (replay 200);
  * contabilidade para AUDITORIA: entregas reais por destinatário, permitindo
    provar que ninguém recebeu o mesmo e-mail duas vezes.

Defina ``SMTP_DEDUP=false`` para desligar a deduplicação do provedor e observar
apenas a proteção feita pelos workers (SETNX no Redis).
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # execução local fora do Docker
from common.carga import chave_idempotencia, falha_permanente  # noqa: E402

LAT_MIN_MS = float(os.getenv("SMTP_LATENCIA_MIN_MS", "5"))
LAT_MAX_MS = float(os.getenv("SMTP_LATENCIA_MAX_MS", "15"))
TAXA_FALHA = float(os.getenv("SMTP_TAXA_FALHA_TRANSITORIA", "0.02"))
DEDUP = os.getenv("SMTP_DEDUP", "true").lower() == "true"

app = FastAPI(title="SMTP Mock", version="1.0.0")


class Email(BaseModel):
    campanha_id: str = Field(max_length=128)
    para: str = Field(max_length=320)
    assunto: str = Field(max_length=500)
    html: str = Field(max_length=200_000)
    rastreio: str = Field(max_length=128)


class Estado:
    """Estado em memória. O event loop é single-thread: operações entre awaits são atômicas."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.inicio = time.time()
        self.global_: Counter = Counter()
        self.por_campanha: dict[str, Counter] = defaultdict(Counter)
        self.entregues: dict[str, str] = {}             # idempotency_key -> message_id
        self.em_andamento: set[str] = set()
        self.entregas_por_destinatario: dict[str, Counter] = defaultdict(Counter)
        self.primeira_entrega: dict[str, float] = {}
        self.ultima_entrega: dict[str, float] = {}


estado = Estado()


def _conta(campanha: str, campo: str) -> None:
    estado.global_[campo] += 1
    estado.por_campanha[campanha][campo] += 1


@app.post("/enviar")
async def enviar(email: Email, idempotency_key: str | None = Header(default=None)):
    chave = idempotency_key or chave_idempotencia(email.campanha_id, email.para)
    _conta(email.campanha_id, "requisicoes")

    if DEDUP:
        if chave in estado.entregues:  # replay idempotente: já entregue, não entrega de novo
            _conta(email.campanha_id, "replays_idempotentes")
            return {"status": "ja_entregue", "message_id": estado.entregues[chave]}
        if chave in estado.em_andamento:  # mesma chave em voo agora: cliente deve tentar depois
            _conta(email.campanha_id, "conflitos_em_andamento")
            return JSONResponse(status_code=409, content={"smtp_code": 451, "erro": "envio em andamento"})
        estado.em_andamento.add(chave)

    try:
        await asyncio.sleep(random.uniform(LAT_MIN_MS, LAT_MAX_MS) / 1000)
        if falha_permanente(email.para):
            _conta(email.campanha_id, "rejeitados_permanentes")
            return JSONResponse(status_code=422, content={"smtp_code": 550, "erro": "mailbox unavailable"})
        if random.random() < TAXA_FALHA:
            _conta(email.campanha_id, "falhas_transitorias")
            return JSONResponse(status_code=503, content={"smtp_code": 451, "erro": "try again later"})

        message_id = uuid.uuid4().hex
        estado.entregues[chave] = message_id
        estado.entregas_por_destinatario[email.campanha_id][email.para] += 1
        agora = time.time()
        estado.primeira_entrega.setdefault(email.campanha_id, agora)
        estado.ultima_entrega[email.campanha_id] = agora
        _conta(email.campanha_id, "entregas")
        return {"status": "entregue", "smtp_code": 250, "message_id": message_id}
    finally:
        estado.em_andamento.discard(chave)


@app.get("/auditoria/{campanha_id}")
async def auditoria(campanha_id: str):
    c = estado.por_campanha.get(campanha_id, Counter())
    entregas = estado.entregas_por_destinatario.get(campanha_id, Counter())
    duplicadas = sum(n - 1 for n in entregas.values() if n > 1)
    return {
        "campanha_id": campanha_id,
        "dedup_provedor_ativo": DEDUP,
        "requisicoes": c["requisicoes"],
        "entregas": c["entregas"],
        "destinatarios_unicos": len(entregas),
        "entregas_duplicadas": duplicadas,
        "destinatarios_com_duplicata": sum(1 for n in entregas.values() if n > 1),
        "replays_idempotentes": c["replays_idempotentes"],
        "conflitos_em_andamento": c["conflitos_em_andamento"],
        "rejeitados_permanentes": c["rejeitados_permanentes"],
        "falhas_transitorias": c["falhas_transitorias"],
        "primeira_entrega": estado.primeira_entrega.get(campanha_id),
        "ultima_entrega": estado.ultima_entrega.get(campanha_id),
    }


@app.get("/metricas")
async def metricas():
    return {"uptime_s": time.time() - estado.inicio, "dedup_provedor_ativo": DEDUP,
            "latencia_ms": [LAT_MIN_MS, LAT_MAX_MS], "taxa_falha_transitoria": TAXA_FALHA,
            "campanhas": len(estado.por_campanha), **estado.global_}


@app.post("/reset")
async def reset():
    estado.reset()
    return {"ok": True}


@app.get("/health")
async def health():
    return {"status": "ok"}
