"""Nó 3 — Worker de disparo (asyncio + aio-pika + aiohttp + redis.asyncio).

Fluxo por mensagem (lote de ~200 destinatários):
  1. consome da fila com ACK MANUAL (prefetch=1 -> distribuição justa entre réplicas);
  2. busca template/assunto da campanha no Redis (com cache local em memória);
  3. processa os destinatários concorrentemente (semáforo) — para cada um:
       a. idempotency_key = sha256(f"{campanha_id}:{email}")
       b. validação (Regex + opt-out)          -> finaliza INVALIDO / OPT_OUT (atômico)
       c. reivindica lease atômico (SET NX PX) -> se já finalizado: PULA (duplicado evitado)
       d. renderiza HTML + hash (CPU)
       e. POST no SMTP Mock com timeout curto + retries com exponential backoff (tenacity)
       f. finaliza ENVIADO / ERRO_SMTP (Lua atômico: estado + contador + fencing)
  4. ACK somente após TODOS os destinatários do lote estarem finalizados.

Se o contêiner morrer no meio do lote, não houve ACK: o RabbitMQ devolve a
mensagem à fila e outro worker a reprocessa. Os destinatários já finalizados são
pulados (SETNX) e o Idempotency-Key enviado ao provedor cobre a janela
"enviou mas morreu antes de gravar ENVIADO".
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import sys
import time
import uuid
from collections import Counter, OrderedDict
from pathlib import Path

import aio_pika
import aiohttp
from redis import asyncio as aioredis
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # execução local fora do Docker
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common.carga import RODADAS_HASH_PADRAO, Destinatario, chave_idempotencia, classificar, renderizar_html  # noqa: E402
from common.fila import ESTADOS_TERMINAIS, declarar_filas, k_idem, k_meta, k_stats, k_template  # noqa: E402
from idempotencia import Idempotencia  # noqa: E402

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/?heartbeat=10")
SMTP_URL = os.getenv("SMTP_URL", "http://localhost:8025").rstrip("/") + "/enviar"
SMTP_TIMEOUT_S = float(os.getenv("SMTP_TIMEOUT_S", "2.0"))
SMTP_MAX_TENTATIVAS = int(os.getenv("SMTP_MAX_TENTATIVAS", "5"))
CONCORRENCIA = int(os.getenv("CONCORRENCIA_POR_WORKER", "16"))
PREFETCH = int(os.getenv("PREFETCH", "1"))
LEASE_MS = int(os.getenv("LEASE_MS", "3000"))
TTL_TERMINAL_S = int(os.getenv("TTL_IDEMPOTENCIA_S", str(7 * 24 * 3600)))
RODADAS_HASH = int(os.getenv("RODADAS_HASH", str(RODADAS_HASH_PADRAO)))
HEARTBEAT_FILE = Path(os.getenv("HEARTBEAT_FILE", "/tmp/worker_heartbeat"))
WORKER_ID = os.getenv("WORKER_ID") or socket.gethostname()[:12]

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format=f"%(asctime)s [worker {WORKER_ID}] %(levelname)s %(message)s")
log = logging.getLogger("worker")


class ErroTransitorio(Exception):
    """Timeout, conexão recusada, 5xx, 409 -> vale tentar de novo."""


class ErroPermanente(Exception):
    """SMTP 550 (caixa inexistente) -> não adianta tentar de novo."""


class LoteAdiado(Exception):
    """O lote não pôde ser concluído agora -> NACK com requeue."""


class Worker:
    def __init__(self, redis: aioredis.Redis, http: aiohttp.ClientSession) -> None:
        self.redis = redis
        self.http = http
        self.idem = Idempotencia(redis, LEASE_MS, TTL_TERMINAL_S)
        self.cache_campanhas: OrderedDict[str, tuple[str, str]] = OrderedDict()
        self.totais: Counter = Counter()

    # ------------------------------------------------------------------ cache
    async def obter_campanha(self, cid: str) -> tuple[str, str] | None:
        if cid in self.cache_campanhas:
            self.cache_campanhas.move_to_end(cid)
            return self.cache_campanhas[cid]
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.get(k_template(cid))
            pipe.hget(k_meta(cid), "assunto")
            template, assunto = await pipe.execute()
        if template is None:
            return None
        self.cache_campanhas[cid] = (template, assunto or "")
        if len(self.cache_campanhas) > 128:
            self.cache_campanhas.popitem(last=False)
        return self.cache_campanhas[cid]

    # ------------------------------------------------------------------- SMTP
    @retry(
        retry=retry_if_exception_type(ErroTransitorio),
        stop=stop_after_attempt(SMTP_MAX_TENTATIVAS),
        wait=wait_exponential_jitter(initial=0.05, max=2.0, jitter=0.05),  # 50ms, 100ms, 200ms, ...
        reraise=True,
    )
    async def enviar_smtp(self, cid: str, para: str, assunto: str, html: str, rastreio: str,
                          idem_key: str) -> float:
        t0 = time.perf_counter()
        try:
            async with self.http.post(
                SMTP_URL,
                json={"campanha_id": cid, "para": para, "assunto": assunto, "html": html, "rastreio": rastreio},
                headers={"Idempotency-Key": idem_key},
            ) as resp:
                status = resp.status
                await resp.read()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            self.totais["smtp_timeouts_ou_conexao"] += 1
            raise ErroTransitorio(repr(exc)) from exc
        if status == 200:
            return (time.perf_counter() - t0) * 1000
        if status in (400, 422):
            raise ErroPermanente(f"HTTP {status}")
        self.totais["smtp_retries"] += 1
        raise ErroTransitorio(f"HTTP {status}")

    # ---------------------------------------------------------- destinatário
    async def processar_destinatario(self, cid: str, d: Destinatario, template: str, assunto: str) -> str:
        idem_key = chave_idempotencia(cid, d.email)
        k, ks = k_idem(idem_key), k_stats(cid)

        motivo = classificar(d)
        if motivo:  # inválido / opt-out: estado terminal gravado uma única vez
            estado = "INVALIDO" if motivo == "invalidos" else "OPT_OUT"
            r = await self.idem.finalizar(k, ks, "", estado, motivo, 0.0, time.time())
            return motivo if r == 1 else "duplicados_evitados"

        token = f"PROC:{WORKER_ID}:{uuid.uuid4().hex[:8]}"
        prazo = time.monotonic() + 2 * LEASE_MS / 1000 + 5
        while True:
            atual = await self.idem.reclamar(k, token)
            if atual == "OK":
                break
            if atual in ESTADOS_TERMINAIS:  # já processado por alguém -> NÃO envia de novo
                await self.redis.hincrby(ks, "duplicados_evitados", 1)
                return "duplicados_evitados"
            # Lease de outro worker: ou ele ainda está enviando, ou morreu e o lease
            # vai expirar em até LEASE_MS. Espera sem bloquear o event loop.
            if time.monotonic() > prazo:
                raise LoteAdiado(f"lease de {atual} não expirou")
            self.totais["esperas_lease"] += 1
            await asyncio.sleep(0.5)

        html, rastreio = renderizar_html(template, d, cid, assunto, RODADAS_HASH)  # CPU-bound
        try:
            latencia_ms = await self.enviar_smtp(cid, d.email, assunto, html, rastreio, idem_key)
            estado, campo = "ENVIADO", "enviados"
        except ErroPermanente:
            estado, campo, latencia_ms = "ERRO_SMTP", "erros_smtp", 0.0
        except ErroTransitorio as exc:
            # Provedor indisponível após todas as tentativas: devolve o lease e adia o
            # lote (em vez de marcar falha definitiva por um problema temporário).
            await self.idem.liberar(k, token)
            raise LoteAdiado(f"SMTP indisponível: {exc}") from exc

        r = await self.idem.finalizar(k, ks, token, estado, campo, latencia_ms, time.time())
        return campo if r == 1 else "leases_perdidos"

    # ------------------------------------------------------------------- lote
    async def processar_lote(self, cid: str, destinatarios: list[Destinatario]) -> Counter:
        campanha = await self.obter_campanha(cid)
        if campanha is None:
            raise LookupError(f"campanha {cid} sem template no cache")
        template, assunto = campanha

        local: Counter = Counter()  # contador LOCAL do lote (sem estado compartilhado)
        limite = asyncio.Semaphore(CONCORRENCIA)

        async def um(d: Destinatario) -> None:
            async with limite:
                local[await self.processar_destinatario(cid, d, template, assunto)] += 1

        resultados = await asyncio.gather(*(um(d) for d in destinatarios), return_exceptions=True)
        falhas = [r for r in resultados if isinstance(r, BaseException)]
        if falhas:
            raise LoteAdiado(f"{len(falhas)} destinatários pendentes; ex.: {falhas[0]!r}")
        return local

    async def on_message(self, msg: aio_pika.abc.AbstractIncomingMessage) -> None:
        try:
            corpo = json.loads(msg.body)
            cid = corpo["campanha_id"]
            destinatarios = [Destinatario(*d) for d in corpo["destinatarios"]]
        except (ValueError, KeyError, TypeError) as exc:
            log.error("mensagem venenosa %s -> DLQ (%s)", msg.message_id, exc)
            await msg.reject(requeue=False)
            return

        t0 = time.perf_counter()
        try:
            local = await self.processar_lote(cid, destinatarios)
        except LookupError as exc:
            log.error("%s -> DLQ", exc)
            await msg.reject(requeue=False)
            return
        except Exception as exc:  # noqa: BLE001 — qualquer falha: devolve o lote à fila
            log.warning("lote %s adiado: %s", msg.message_id, exc)
            await asyncio.sleep(1.0)
            await msg.nack(requeue=True)
            return

        await msg.ack()  # ACK só depois de todos os destinatários finalizados
        self.totais.update(local)
        self.totais["lotes"] += 1
        log.info("lote %s%s ok em %.2fs %s", msg.message_id, " (REENTREGUE)" if msg.redelivered else "",
                 time.perf_counter() - t0, dict(local))


async def heartbeat(parar: asyncio.Event) -> None:
    """Arquivo usado pelo healthcheck do contêiner (prova que o event loop está vivo)."""
    while not parar.is_set():
        HEARTBEAT_FILE.write_text(str(time.time()))
        try:
            await asyncio.wait_for(parar.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    http = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=SMTP_TIMEOUT_S),
        connector=aiohttp.TCPConnector(limit=CONCORRENCIA * 2),
    )
    conexao = await aio_pika.connect_robust(RABBITMQ_URL, client_properties={"connection_name": f"worker-{WORKER_ID}"})
    canal = await conexao.channel()
    await canal.set_qos(prefetch_count=PREFETCH)
    fila = await declarar_filas(canal)

    worker = Worker(redis, http)
    parar = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, parar.set)

    tarefa_hb = asyncio.create_task(heartbeat(parar))
    tag = await fila.consume(worker.on_message)
    log.info("pronto: concorrência=%d prefetch=%d lease=%dms smtp=%s", CONCORRENCIA, PREFETCH, LEASE_MS, SMTP_URL)

    await parar.wait()
    log.info("encerrando (SIGTERM). Mensagens sem ACK voltarão para a fila. Totais: %s", dict(worker.totais))
    await fila.cancel(tag)
    await tarefa_hb
    await conexao.close()
    await http.close()
    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
