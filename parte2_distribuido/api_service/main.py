"""Nó 1 — API de Campanhas (FastAPI).

POST /campanhas                -> síncrono (request/response, contrato Pydantic, timeout)
                                  1. grava metadados + template no Redis (cache de leitura rápida)
                                  2. quebra os destinatários em lotes e publica no RabbitMQ
                                     (mensagens persistentes + publisher confirms)
GET  /campanhas/{id}/status    -> CONSISTÊNCIA EVENTUAL: lê contadores de progresso do Redis
                                  sem bloquear os workers (pode estar alguns ms atrasado)
GET  /health                   -> usado pelo healthcheck do Docker
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import aio_pika
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from redis import asyncio as aioredis

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # execução local fora do Docker
from common.carga import TEMPLATE_PADRAO  # noqa: E402
from common.fila import CATEGORIAS, declarar_filas, FILA_LOTES, k_meta, k_stats, k_template  # noqa: E402

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
TIMEOUT_REQUISICAO_S = float(os.getenv("TIMEOUT_REQUISICAO_S", "60"))
TTL_CAMPANHA_S = int(os.getenv("TTL_CAMPANHA_S", str(7 * 24 * 3600)))
PUBLICACOES_SIMULTANEAS = int(os.getenv("PUBLICACOES_SIMULTANEAS", "64"))


# ---------------------------------------------------------------------------
# Contratos (Pydantic)
# ---------------------------------------------------------------------------


class DestinatarioIn(BaseModel):
    id: int
    nome: str = Field(max_length=200)
    # Intencionalmente `str` e não `EmailStr`: validar a sintaxe é parte da carga
    # de trabalho do worker (e os inválidos precisam ser contabilizados).
    email: str = Field(max_length=320)
    segmento: str = Field(max_length=100)
    oferta_personalizada: str = Field(max_length=300)
    opt_out: bool = False


class CampanhaIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nome: str = Field(min_length=1, max_length=200)
    assunto: str = Field(min_length=1, max_length=300)
    template_html: str | None = Field(default=None, max_length=200_000)
    tamanho_lote: int = Field(default=200, ge=1, le=5_000)
    destinatarios: list[DestinatarioIn] = Field(min_length=1, max_length=200_000)


class CampanhaCriada(BaseModel):
    campanha_id: str
    total_destinatarios: int
    total_lotes: int
    tamanho_lote: int
    criada_em: float
    publicacao_s: float
    status_url: str


class StatusCampanha(BaseModel):
    campanha_id: str
    nome: str
    status: str
    consistencia: str = "eventual"
    total: int
    processados: int
    progresso_pct: float
    enviados: int
    invalidos: int
    opt_out: int
    erros_smtp: int
    duplicados_evitados: int
    leases_perdidos: int
    latencia_media_smtp_ms: float | None
    fila_mensagens_prontas: int | None
    criada_em: float
    atualizada_em: float | None
    duracao_s: float


# ---------------------------------------------------------------------------
# Ciclo de vida (conexões reaproveitadas por todas as requisições)
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    app.state.amqp = await aio_pika.connect_robust(RABBITMQ_URL, client_properties={"connection_name": "api"})
    app.state.canal_pub = await app.state.amqp.channel(publisher_confirms=True)
    await declarar_filas(app.state.canal_pub)
    app.state.canal_consulta = await app.state.amqp.channel()
    yield
    await app.state.amqp.close()
    await app.state.redis.aclose()


app = FastAPI(title="API de Campanhas — Envio em Massa", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def timeout_middleware(request: Request, call_next):
    """Timeout definido no servidor: nenhuma requisição prende a API indefinidamente."""
    try:
        return await asyncio.wait_for(call_next(request), timeout=TIMEOUT_REQUISICAO_S)
    except asyncio.TimeoutError:
        return JSONResponse(status_code=504, content={"detail": f"timeout de {TIMEOUT_REQUISICAO_S}s excedido"})


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.post("/campanhas", response_model=CampanhaCriada, status_code=202)
async def criar_campanha(campanha: CampanhaIn, request: Request):
    redis: aioredis.Redis = request.app.state.redis
    canal: aio_pika.abc.AbstractChannel = request.app.state.canal_pub

    cid = uuid.uuid4().hex
    criada_em = time.time()
    total = len(campanha.destinatarios)
    lotes = [campanha.destinatarios[i:i + campanha.tamanho_lote] for i in range(0, total, campanha.tamanho_lote)]

    # 1) Metadados + template no cache ANTES de publicar (o worker precisa encontrá-los).
    async with redis.pipeline(transaction=True) as pipe:
        pipe.hset(k_meta(cid), mapping={
            "nome": campanha.nome, "assunto": campanha.assunto, "total": total,
            "total_lotes": len(lotes), "tamanho_lote": campanha.tamanho_lote,
            "criada_em": criada_em, "publicacao": "em_andamento",
        })
        pipe.set(k_template(cid), campanha.template_html or TEMPLATE_PADRAO)
        pipe.hset(k_stats(cid), mapping={c: 0 for c in (*CATEGORIAS, "duplicados_evitados", "leases_perdidos")})
        for k in (k_meta(cid), k_template(cid), k_stats(cid)):
            pipe.expire(k, TTL_CAMPANHA_S)
        await pipe.execute()

    # 2) Publicação dos lotes: mensagens persistentes + publisher confirms
    #    (o publish só retorna após o broker confirmar que gravou a mensagem).
    t0 = time.perf_counter()
    limite = asyncio.Semaphore(PUBLICACOES_SIMULTANEAS)

    async def publicar(n: int, lote: list[DestinatarioIn]) -> None:
        corpo = json.dumps({
            "campanha_id": cid, "lote": n, "total_lotes": len(lotes),
            "destinatarios": [[d.id, d.nome, d.email, d.segmento, d.oferta_personalizada, d.opt_out] for d in lote],
        }, ensure_ascii=False).encode()
        async with limite:
            await canal.default_exchange.publish(
                aio_pika.Message(corpo, delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                                 content_type="application/json", message_id=f"{cid}:{n}"),
                routing_key=FILA_LOTES,
            )

    await asyncio.gather(*(publicar(n, lote) for n, lote in enumerate(lotes)))
    await redis.hset(k_meta(cid), "publicacao", "concluida")

    return CampanhaCriada(
        campanha_id=cid, total_destinatarios=total, total_lotes=len(lotes),
        tamanho_lote=campanha.tamanho_lote, criada_em=criada_em,
        publicacao_s=time.perf_counter() - t0, status_url=f"/campanhas/{cid}/status",
    )


@app.get("/campanhas/{campanha_id}/status", response_model=StatusCampanha)
async def status_campanha(campanha_id: str, request: Request):
    redis: aioredis.Redis = request.app.state.redis
    async with redis.pipeline(transaction=False) as pipe:
        pipe.hgetall(k_meta(campanha_id))
        pipe.hgetall(k_stats(campanha_id))
        meta, stats = await pipe.execute()
    if not meta:
        raise HTTPException(status_code=404, detail="campanha não encontrada")

    def n(campo: str) -> int:
        return int(float(stats.get(campo, 0)))

    total = int(meta["total"])
    processados = sum(n(c) for c in CATEGORIAS)
    criada_em = float(meta["criada_em"])
    atualizada_em = float(stats["atualizada_em"]) if "atualizada_em" in stats else None
    concluida = processados >= total
    fim = atualizada_em if (concluida and atualizada_em) else time.time()

    prontas = None
    try:  # profundidade da fila (mensagens prontas, ainda não entregues a nenhum worker)
        fila = await request.app.state.canal_consulta.declare_queue(FILA_LOTES, passive=True)
        prontas = fila.declaration_result.message_count
    except Exception:  # noqa: BLE001 — status nunca deve falhar por causa da métrica auxiliar
        request.app.state.canal_consulta = await request.app.state.amqp.channel()

    return StatusCampanha(
        campanha_id=campanha_id, nome=meta.get("nome", ""),
        status="concluida" if concluida else "em_andamento",
        total=total, processados=processados,
        progresso_pct=round(100 * processados / total, 2) if total else 100.0,
        enviados=n("enviados"), invalidos=n("invalidos"), opt_out=n("opt_out"), erros_smtp=n("erros_smtp"),
        duplicados_evitados=n("duplicados_evitados"), leases_perdidos=n("leases_perdidos"),
        latencia_media_smtp_ms=(float(stats.get("latencia_total_ms", 0)) / n("enviados")) if n("enviados") else None,
        fila_mensagens_prontas=prontas, criada_em=criada_em, atualizada_em=atualizada_em,
        duracao_s=round(fim - criada_em, 3),
    )


@app.get("/health")
async def health(request: Request):
    try:
        await request.app.state.redis.ping()
        if request.app.state.amqp.is_closed:
            raise RuntimeError("AMQP desconectado")
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=503, content={"status": "degradado", "erro": str(exc)})
    return {"status": "ok"}
