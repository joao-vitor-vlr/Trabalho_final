"""Núcleo da Parte 1: chunks, worker, MapReduce local, verificação e CLI.

Separação de dependências
-------------------------
  DEPENDENTE (sequencial)       -> carregar o CSV  |  consolidar o relatório
  INDEPENDENTE (paralelizável)  -> validar, renderizar + hash, disparar (por e-mail)

Os três executores (sequencial, threads, processos) chamam EXATAMENTE a mesma
função ``processar_chunk``; só muda quem a executa. Isso torna a comparação justa.

Zero lock contention
--------------------
Cada chamada de ``processar_chunk`` acumula um ``collections.Counter`` LOCAL e o
devolve ao final. Nenhum estado é compartilhado durante o processamento, então
não há ``Lock``. O processo principal faz o "reduce" somando os contadores.
"""

from __future__ import annotations

import argparse
import cProfile
import json
import pstats
import random
import sys
import time
import uuid
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence, TypeVar

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:  # permite executar os scripts por caminho
    sys.path.insert(0, str(RAIZ))

from common.carga import (  # noqa: E402
    RODADAS_HASH_PADRAO,
    TEMPLATE_PADRAO,
    Destinatario,
    chave_idempotencia,
    classificar,
    renderizar_html,
)
from common.csv_io import carregar_destinatarios  # noqa: E402
from common.smtp_cliente import ErroSMTP, SMTPHttp, SMTPSimulado, enviar_com_retry  # noqa: E402

CATEGORIAS = ("enviados", "invalidos", "opt_out", "erros_smtp")
CSV_PADRAO = RAIZ / "dataset" / "destinatarios.csv"

T = TypeVar("T")


@dataclass(frozen=True)
class ConfigExecucao:
    """Configuração imutável e picklável enviada a cada worker."""

    campanha_id: str = "campanha-local"
    assunto: str = "Oferta exclusiva para você"
    template: str = TEMPLATE_PADRAO
    rodadas_hash: int = RODADAS_HASH_PADRAO
    latencia_min_ms: float = 5.0
    latencia_max_ms: float = 15.0
    taxa_falha_transitoria: float = 0.02
    max_tentativas: int = 4
    smtp_url: str | None = None
    seed: int = 42


# ---------------------------------------------------------------------------
# Divisão em chunks
# ---------------------------------------------------------------------------


def dividir_em_chunks(itens: Sequence[T], n_chunks: int) -> list[Sequence[T]]:
    """Divide em ``n_chunks`` sub-lotes contíguos e EQUILIBRADOS (tamanhos diferem em no máx. 1).

    Ex.: 10 itens em 3 chunks -> [4, 3, 3]. Usar poucos chunks grandes (em vez de
    uma tarefa por e-mail) amortiza o custo de submissão/pickle de cada tarefa;
    usar alguns chunks por worker (``chunks_por_worker``) mantém o balanceamento
    caso um chunk demore mais que outro.
    """
    if not itens:
        return []
    n_chunks = max(1, min(n_chunks, len(itens)))
    base, resto = divmod(len(itens), n_chunks)
    chunks, inicio = [], 0
    for i in range(n_chunks):
        fim = inicio + base + (1 if i < resto else 0)
        chunks.append(itens[inicio:fim])
        inicio = fim
    return chunks


# ---------------------------------------------------------------------------
# Worker (parte independente / paralelizável)
# ---------------------------------------------------------------------------


def _criar_cliente(cfg: ConfigExecucao, seed: int):
    if cfg.smtp_url:
        return SMTPHttp(cfg.smtp_url)
    return SMTPSimulado(cfg.latencia_min_ms, cfg.latencia_max_ms, cfg.taxa_falha_transitoria, seed)


def processar_chunk(indice: int, chunk: Sequence[Destinatario], cfg: ConfigExecucao) -> Counter:
    """Processa um sub-lote e devolve um Counter LOCAL (sem estado compartilhado, sem Lock)."""
    local: Counter = Counter()
    seed = cfg.seed * 1_000_003 + indice
    rng = random.Random(seed)
    cliente = _criar_cliente(cfg, seed)
    perf, cpu = time.perf_counter, time.thread_time
    try:
        for d in chunk:
            t0, c0 = perf(), cpu()
            # 1) validação
            motivo = classificar(d)
            if motivo:
                local[motivo] += 1
                local["tempo_cpu_wall"] += perf() - t0
                local["tempo_cpu_thread"] += cpu() - c0
                continue
            # 2) renderização + hash (CPU-bound, segura o GIL)
            html, rastreio = renderizar_html(cfg.template, d, cfg.campanha_id, cfg.assunto, cfg.rodadas_hash)
            t1 = perf()
            local["tempo_cpu_wall"] += t1 - t0          # inclui espera pelo GIL (threads)
            local["tempo_cpu_thread"] += cpu() - c0     # CPU efetivamente consumida
            local["renderizados"] += 1
            # 3) disparo (I/O-bound, libera o GIL)
            try:
                lat, retries = enviar_com_retry(
                    cliente, cfg.campanha_id, d.email, cfg.assunto, html, rastreio,
                    chave_idempotencia(cfg.campanha_id, d.email),
                    max_tentativas=cfg.max_tentativas, rng=rng,
                )
                local["enviados"] += 1
                local["latencia_total"] += lat
                local["retries"] += retries
            except ErroSMTP:
                local["erros_smtp"] += 1
            local["tempo_io_wall"] += perf() - t1
    finally:
        local["atraso_pos_io"] += getattr(cliente, "atraso_pos_io", 0.0)
        cliente.fechar()
    local["chunks_processados"] += 1
    return local


# ---------------------------------------------------------------------------
# Reduce + verificação de correção (parte dependente)
# ---------------------------------------------------------------------------


def consolidar(parciais: list[Counter]) -> Counter:
    total: Counter = Counter()
    for p in parciais:
        total.update(p)  # update() soma e preserva zeros (o operador + descartaria)
    return total


def verificar_correcao(total: Counter, total_registros: int) -> None:
    soma = sum(int(total[k]) for k in CATEGORIAS)
    # Velocidade sem correção não vale: todo registro deve cair em exatamente 1 categoria.
    assert soma == total_registros, (
        f"FALHA DE CORREÇÃO: enviados+invalidos+opt_out+erros_smtp = {soma} != {total_registros}"
    )


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------


def executar(modo: str, n_workers: int, cfg: ConfigExecucao, csv_path: Path = CSV_PADRAO,
             chunks_por_worker: int = 4) -> dict:
    if modo not in ("sequencial", "threads", "processos"):
        raise ValueError(modo)
    t_inicio = time.perf_counter()

    destinatarios = carregar_destinatarios(csv_path)            # DEPENDENTE
    t_carga = time.perf_counter()

    if modo == "sequencial":
        n_workers, chunks = 1, [destinatarios]
        parciais = [processar_chunk(0, destinatarios, cfg)]
    else:                                                        # INDEPENDENTE
        chunks = dividir_em_chunks(destinatarios, n_workers * chunks_por_worker)
        Pool = ThreadPoolExecutor if modo == "threads" else ProcessPoolExecutor
        with Pool(max_workers=n_workers) as executor:
            futuros = [executor.submit(processar_chunk, i, c, cfg) for i, c in enumerate(chunks)]
            parciais = [f.result() for f in as_completed(futuros)]
    t_paralelo = time.perf_counter()

    total = consolidar(parciais)                                 # DEPENDENTE
    verificar_correcao(total, len(destinatarios))
    t_fim = time.perf_counter()

    n = len(destinatarios)
    renderizados = max(1, int(total["renderizados"]))
    tempo_total = t_fim - t_inicio
    return {
        "modo": modo,
        "workers": n_workers,
        "chunks": len(chunks),
        "total_registros": n,
        "smtp": cfg.smtp_url or f"simulado {cfg.latencia_min_ms:g}-{cfg.latencia_max_ms:g}ms",
        "rodadas_hash": cfg.rodadas_hash,
        "contadores": {k: int(total[k]) for k in (*CATEGORIAS, "retries", "renderizados")},
        "correcao_ok": True,
        "tempos_s": {
            "total": tempo_total,
            "carga_csv": t_carga - t_inicio,
            "fase_paralela": t_paralelo - t_carga,
            "consolidacao": t_fim - t_paralelo,
        },
        "throughput_registros_s": n / tempo_total,
        "throughput_envios_s": total["enviados"] / tempo_total,
        "metricas_ms": {
            "latencia_smtp_media": 1000 * total["latencia_total"] / max(1, total["enviados"]),
            "cpu_wall_por_email": 1000 * total["tempo_cpu_wall"] / renderizados,
            "cpu_thread_por_email": 1000 * total["tempo_cpu_thread"] / renderizados,
            # espera pelo GIL dentro da fase de CPU (preempção a cada 5 ms de switch interval)
            "espera_gil_cpu_por_email": 1000 * max(0.0, total["tempo_cpu_wall"] - total["tempo_cpu_thread"]) / renderizados,
            # atraso para retomar após o I/O (readquirir o GIL + folga do timer do SO);
            # no sequencial representa só a folga do SO -> serve de linha de base
            "atraso_pos_io_por_email": 1000 * total["atraso_pos_io"] / renderizados,
            "io_wall_por_email": 1000 * total["tempo_io_wall"] / renderizados,
        },
    }


def imprimir(r: dict) -> None:
    c, t, m = r["contadores"], r["tempos_s"], r["metricas_ms"]
    print(f"\n=== {r['modo'].upper()} | workers={r['workers']} | chunks={r['chunks']} | smtp={r['smtp']} ===")
    print(f"  registros={r['total_registros']}  enviados={c['enviados']}  invalidos={c['invalidos']}  "
          f"opt_out={c['opt_out']}  erros_smtp={c['erros_smtp']}  retries={c['retries']}")
    print(f"  [OK] verificação: soma das categorias == {r['total_registros']}")
    print(f"  tempo total={t['total']:.3f}s (csv={t['carga_csv']:.3f}s | paralelo={t['fase_paralela']:.3f}s "
          f"| consolidação={t['consolidacao'] * 1000:.2f}ms)")
    print(f"  throughput={r['throughput_registros_s']:.1f} registros/s ({r['throughput_envios_s']:.1f} envios/s)")
    print(f"  por e-mail: CPU wall={m['cpu_wall_por_email']:.3f}ms  CPU real={m['cpu_thread_por_email']:.3f}ms  "
          f"I/O wall={m['io_wall_por_email']:.3f}ms  atraso pós-I/O (GIL+SO)={m['atraso_pos_io_por_email']:.3f}ms  "
          f"espera GIL na CPU={m['espera_gil_cpu_por_email']:.3f}ms")


# ---------------------------------------------------------------------------
# CLI compartilhada
# ---------------------------------------------------------------------------


def main(modo: str, argv: list[str] | None = None) -> list[dict]:
    p = argparse.ArgumentParser(description=f"Envio em massa — executor {modo}")
    p.add_argument("--csv", type=Path, default=CSV_PADRAO)
    if modo != "sequencial":
        p.add_argument("--workers", type=int, nargs="+", default=[2, 4, 8],
                       help="quantidades de workers a testar (padrão: 2 4 8)")
        p.add_argument("--chunks-por-worker", type=int, default=4)
    p.add_argument("--rodadas-hash", type=int, default=RODADAS_HASH_PADRAO,
                   help="fator de custo CPU da etapa de renderização")
    p.add_argument("--latencia-min-ms", type=float, default=5.0)
    p.add_argument("--latencia-max-ms", type=float, default=15.0)
    p.add_argument("--taxa-falha-transitoria", type=float, default=0.02)
    p.add_argument("--smtp-url", default=None,
                   help="usa o smtp_mock HTTP real (ex.: http://localhost:8025) em vez do simulado")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--profile", action="store_true", help="perfila com cProfile (salva .prof em resultados/profiles)")
    p.add_argument("--json-out", type=Path, default=None, help="salva os resultados em JSON")
    a = p.parse_args(argv)

    lista_workers = getattr(a, "workers", [1])
    resultados = []
    for n in lista_workers:
        cfg = ConfigExecucao(
            campanha_id=f"local-{modo}-{n}-{uuid.uuid4().hex[:8]}",
            rodadas_hash=a.rodadas_hash,
            latencia_min_ms=a.latencia_min_ms,
            latencia_max_ms=a.latencia_max_ms,
            taxa_falha_transitoria=a.taxa_falha_transitoria,
            smtp_url=a.smtp_url,
            seed=a.seed,
        )
        kwargs = {"csv_path": a.csv}
        if modo != "sequencial":
            kwargs["chunks_por_worker"] = a.chunks_por_worker

        if a.profile:
            prof = cProfile.Profile()
            r = prof.runcall(executar, modo, n, cfg, **kwargs)
            destino = RAIZ / "resultados" / "profiles" / f"{modo}_{n}.prof"
            destino.parent.mkdir(parents=True, exist_ok=True)
            prof.dump_stats(destino)
            print(f"\n[cProfile] {destino} (top 12 por tempo cumulativo"
                  f"{'; threads/processos: perfil da thread principal' if modo != 'sequencial' else ''})")
            pstats.Stats(prof).sort_stats("cumulative").print_stats(12)
        else:
            r = executar(modo, n, cfg, **kwargs)
        r["config"] = {k: v for k, v in asdict(cfg).items() if k != "template"}
        imprimir(r)
        resultados.append(r)

    if a.json_out:
        a.json_out.parent.mkdir(parents=True, exist_ok=True)
        a.json_out.write_text(json.dumps(resultados, indent=2, ensure_ascii=False))
        print(f"\n[json] {a.json_out}")
    return resultados
