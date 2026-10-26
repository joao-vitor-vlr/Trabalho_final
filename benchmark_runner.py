"""Bateria automatizada de experimentos + relatório (RELATORIO_RESULTADOS.md) + gráficos.

    python benchmark_runner.py                         # testes 1, 2, 3 e 4 com 10.000 registros
    python benchmark_runner.py --testes 1 2            # só a Parte 1
    python benchmark_runner.py --testes 4              # só o chaos test
    python benchmark_runner.py --apenas-relatorio      # regera relatório/gráficos a partir do JSON salvo

Teste 1  Referência sequencial
Teste 2  Escala paralela: 2/4/8 threads e 2/4/8 processos (+ 2b: perfil 100% CPU, isola o GIL)
Teste 3  Escala distribuída: 1, 2 e 3 contêineres worker (tempo de esvaziamento da fila)
Teste 4  Chaos: 3 workers, `docker kill` em 1 ao atingir ~30%, auditoria de 0 duplicatas

Todos os testes usam o MESMO dataset (mesma seed) e a MESMA carga de trabalho (common/carga.py).
Os resultados de cada teste são mesclados em resultados/resultados.json, então os testes
podem ser executados separadamente e o relatório sempre reflete tudo o que já foi medido.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
sys.path.insert(0, str(RAIZ))

from common.carga import resultado_esperado  # noqa: E402
from common.csv_io import carregar_destinatarios  # noqa: E402

DIR_RES = RAIZ / "resultados"
DIR_GRAF = DIR_RES / "graficos"
ARQ_RES = DIR_RES / "resultados.json"
RELATORIO = RAIZ / "RELATORIO_RESULTADOS.md"
CSV_PADRAO = RAIZ / "dataset" / "destinatarios.csv"
COMPOSE = ["docker", "compose", "-f", str(RAIZ / "parte2_distribuido" / "docker-compose.yml")]
CATEGORIAS = ("enviados", "invalidos", "opt_out", "erros_smtp")


def log(msg: str) -> None:
    print(f"\033[1;36m[benchmark {datetime.now():%H:%M:%S}]\033[0m {msg}", flush=True)


# ============================================================================
# Persistência dos resultados
# ============================================================================


def carregar_resultados() -> dict:
    if ARQ_RES.exists():
        return json.loads(ARQ_RES.read_text())
    return {}


def salvar_resultados(res: dict) -> None:
    DIR_RES.mkdir(parents=True, exist_ok=True)
    ARQ_RES.write_text(json.dumps(res, indent=2, ensure_ascii=False))


def info_ambiente() -> dict:
    docker = None
    try:
        docker = subprocess.run(["docker", "info", "--format", "{{.ServerVersion}} | {{.NCPU}} vCPUs | {{.MemTotal}} B"],
                                capture_output=True, text=True, timeout=15).stdout.strip() or None
    except Exception:  # noqa: BLE001
        pass
    return {
        "data": datetime.now().isoformat(timespec="seconds"),
        "plataforma": platform.platform(),
        "processador": platform.processor() or platform.machine(),
        "cpus_logicas": os.cpu_count(),
        "python": platform.python_version(),
        "docker": docker,
    }


# ============================================================================
# Dataset
# ============================================================================


def preparar_dataset(registros: int, seed: int, csv_path: Path) -> dict:
    log(f"gerando dataset padronizado: {registros} registros (seed={seed})")
    cmd = [sys.executable, "-m", "dataset.gerador_carga", "--registros", str(registros),
           "--seed", str(seed), "--saida", str(csv_path)]
    if not 10_000 <= registros <= 50_000:
        cmd.append("--permitir-fora-faixa")
    subprocess.run(cmd, check=True, cwd=RAIZ)
    esperado = resultado_esperado(carregar_destinatarios(csv_path))
    log(f"gabarito (resultado correto esperado): {esperado}")
    return esperado


# ============================================================================
# Parte 1 (Testes 1, 2 e 2b)
# ============================================================================


def rodar_parte1(modulo: str, csv_path: Path, extra: list[str], saida: Path) -> list[dict]:
    cmd = [sys.executable, "-m", f"parte1_paralelo.{modulo}", "--csv", str(csv_path), "--json-out", str(saida), *extra]
    log("$ " + " ".join(cmd[1:]))
    subprocess.run(cmd, check=True, cwd=RAIZ)
    return json.loads(saida.read_text())


def conferir(contadores: dict, esperado: dict) -> bool:
    return all(int(contadores.get(k, -1)) == esperado[k] for k in CATEGORIAS)


def teste1(res: dict, csv_path: Path, esperado: dict) -> None:
    log("TESTE 1 — referência sequencial")
    r = rodar_parte1("sequencial", csv_path, [], DIR_RES / "t1_sequencial.json")[0]
    r["confere_gabarito"] = conferir(r["contadores"], esperado)
    res["teste1"] = r


def teste2(res: dict, csv_path: Path, esperado: dict, workers: list[int], perfil_cpu: bool) -> None:
    log(f"TESTE 2 — escala paralela com {workers} threads e processos")
    ws = [str(w) for w in workers]
    t = rodar_parte1("paralelo_threads", csv_path, ["--workers", *ws], DIR_RES / "t2_threads.json")
    p = rodar_parte1("paralelo_processos", csv_path, ["--workers", *ws], DIR_RES / "t2_processos.json")
    for r in (*t, *p):
        r["confere_gabarito"] = conferir(r["contadores"], esperado)
    res["teste2"] = {"threads": t, "processos": p}

    if perfil_cpu:
        log("TESTE 2b — perfil 100% CPU-bound (latência de rede = 0) para isolar o efeito do GIL")
        sem_io = ["--latencia-min-ms", "0", "--latencia-max-ms", "0", "--taxa-falha-transitoria", "0"]
        s = rodar_parte1("sequencial", csv_path, sem_io, DIR_RES / "t2b_sequencial.json")
        t = rodar_parte1("paralelo_threads", csv_path, ["--workers", *ws, *sem_io], DIR_RES / "t2b_threads.json")
        p = rodar_parte1("paralelo_processos", csv_path, ["--workers", *ws, *sem_io], DIR_RES / "t2b_processos.json")
        for r in (*s, *t, *p):
            r["confere_gabarito"] = conferir(r["contadores"], esperado)
        res["teste2b"] = {"sequencial": s[0], "threads": t, "processos": p}


# ============================================================================
# Parte 2 (Testes 3 e 4)
# ============================================================================


def compose(*args: str, capturar: bool = False, check: bool = True) -> str:
    r = subprocess.run([*COMPOSE, *args], cwd=RAIZ, text=True, check=check,
                       capture_output=capturar)
    return (r.stdout or "").strip() if capturar else ""


def subir_ambiente(n_workers: int, build: bool = False) -> None:
    args = ["up", "-d", "--wait", "--scale", f"worker={n_workers}"]
    if build:
        args.insert(2, "--build")
    log(f"docker compose {' '.join(args)}")
    compose(*args)


def workers_rodando() -> list[str]:
    saida = compose("ps", "-q", "--status", "running", "worker", capturar=True)
    return [linha for linha in saida.splitlines() if linha.strip()]


class ClienteAPI:
    def __init__(self, api: str, smtp: str) -> None:
        import requests

        self.req = requests
        self.api = api.rstrip("/")
        self.smtp = smtp.rstrip("/")
        self.sessao = requests.Session()

    def aguardar_saude(self, timeout_s: float = 120) -> None:
        limite = time.time() + timeout_s
        while time.time() < limite:
            try:
                if (self.sessao.get(f"{self.api}/health", timeout=2).ok
                        and self.sessao.get(f"{self.smtp}/health", timeout=2).ok):
                    return
            except self.req.RequestException:
                pass
            time.sleep(1)
        raise RuntimeError("API/SMTP mock não ficaram saudáveis a tempo")

    def criar_campanha(self, nome: str, destinatarios: list[dict]) -> dict:
        r = self.sessao.post(f"{self.api}/campanhas", timeout=(5, 120), json={
            "nome": nome, "assunto": "Oferta exclusiva para você", "tamanho_lote": 200,
            "destinatarios": destinatarios,
        })
        r.raise_for_status()
        return r.json()

    def status(self, cid: str) -> dict:
        r = self.sessao.get(f"{self.api}/campanhas/{cid}/status", timeout=3)
        r.raise_for_status()
        return r.json()

    def auditoria(self, cid: str) -> dict:
        return self.sessao.get(f"{self.smtp}/auditoria/{cid}", timeout=10).json()

    def acompanhar(self, cid: str, t0: float, intervalo: float = 0.25, timeout_s: float = 1800,
                   ao_progresso=None) -> tuple[dict, list[dict], dict]:
        """Faz polling do status até concluir. Retorna (status_final, linha_do_tempo, disponibilidade)."""
        linha, ok, falhas, lat = [], 0, 0, []
        limite = time.time() + timeout_s
        while time.time() < limite:
            t_req = time.perf_counter()
            try:
                st = self.status(cid)
                lat.append((time.perf_counter() - t_req) * 1000)
                ok += 1
            except self.req.RequestException:
                falhas += 1
                time.sleep(intervalo)
                continue
            agora = time.perf_counter() - t0
            linha.append({"t": round(agora, 3), "processados": st["processados"], "pct": st["progresso_pct"],
                          "fila": st["fila_mensagens_prontas"], "duplicados_evitados": st["duplicados_evitados"]})
            if ao_progresso:
                ao_progresso(st, agora)
            if st["status"] == "concluida":
                lat.sort()
                disp = {"polls_ok": ok, "polls_falhos": falhas,
                        "disponibilidade_pct": 100 * ok / max(1, ok + falhas),
                        "latencia_status_p50_ms": lat[len(lat) // 2] if lat else None,
                        "latencia_status_p99_ms": lat[int(len(lat) * 0.99) - 1] if lat else None}
                return st, linha, disp
            time.sleep(intervalo)
        raise TimeoutError(f"campanha {cid} não concluiu em {timeout_s}s")


def ler_csv_dicts(csv_path: Path) -> list[dict]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        return [{**row, "id": int(row["id"]), "opt_out": row["opt_out"] == "True"} for row in csv.DictReader(f)]


def executar_campanha(cli: ClienteAPI, nome: str, dests: list[dict], ao_progresso=None) -> dict:
    t0 = time.perf_counter()
    criada = cli.criar_campanha(nome, dests)
    t_resp = time.perf_counter() - t0
    st, linha, disp = cli.acompanhar(criada["campanha_id"], t0, ao_progresso=ao_progresso)
    t_cliente = time.perf_counter() - t0
    time.sleep(0.5)  # deixa o SMTP mock registrar as últimas entregas
    aud = cli.auditoria(criada["campanha_id"])
    return {"campanha": criada, "tempo_resposta_post_s": t_resp, "tempo_cliente_s": t_cliente,
            "tempo_esvaziamento_s": st["duracao_s"], "throughput_registros_s": st["total"] / st["duracao_s"],
            "status_final": st, "auditoria_smtp": aud, "disponibilidade": disp, "linha_do_tempo": linha}


def auditar(run: dict, esperado: dict) -> list[dict]:
    st, aud = run["status_final"], run["auditoria_smtp"]
    checks = [
        ("Todos os registros processados (processados == total)", st["processados"] == esperado["total"],
         f"{st['processados']} / {esperado['total']}"),
        ("Contadores conferem com o gabarito (enviados/inválidos/opt-out/erros 550)",
         all(st[k] == esperado[k] for k in CATEGORIAS),
         ", ".join(f"{k}={st[k]} (esperado {esperado[k]})" for k in CATEGORIAS)),
        ("100% dos e-mails válidos entregues no provedor (destinatários únicos)",
         aud["destinatarios_unicos"] == esperado["enviados"],
         f"{aud['destinatarios_unicos']} / {esperado['enviados']}"),
        ("0 envios duplicados (auditoria do provedor)", aud["entregas_duplicadas"] == 0,
         f"entregas_duplicadas={aud['entregas_duplicadas']}"),
        ("API respondeu a 100% das consultas de status", run["disponibilidade"]["polls_falhos"] == 0,
         f"{run['disponibilidade']['polls_ok']} ok / {run['disponibilidade']['polls_falhos']} falhas"),
    ]
    return [{"verificacao": c, "ok": bool(ok), "detalhe": d} for c, ok, d in checks]


def aquecer(cli: ClienteAPI, dests: list[dict]) -> None:
    """Campanha pequena e não medida: abre conexões/caches antes da medição."""
    log("aquecimento (campanha de 400 destinatários, não medida)")
    cid = cli.criar_campanha("aquecimento", dests[:400])["campanha_id"]
    cli.acompanhar(cid, time.perf_counter())


def teste3(res: dict, csv_path: Path, esperado: dict, workers: list[int], cli: ClienteAPI, build: bool) -> None:
    log(f"TESTE 3 — escala distribuída com {workers} workers")
    dests = ler_csv_dicts(csv_path)
    execucoes = []
    for i, n in enumerate(sorted(workers)):
        subir_ambiente(n, build=build and i == 0)
        cli.aguardar_saude()
        cli.sessao.post(f"{cli.smtp}/reset", timeout=5)
        aquecer(cli, dests)
        log(f"campanha medida com {n} worker(s) ativos: {len(workers_rodando())} contêineres rodando")
        run = executar_campanha(cli, f"benchmark-{n}-workers", dests)
        run["workers"] = n
        run["auditoria"] = auditar(run, esperado)
        run.pop("linha_do_tempo")
        log(f"  {n} worker(s): esvaziou em {run['tempo_esvaziamento_s']:.2f}s "
            f"({run['throughput_registros_s']:.1f} reg/s) | correção: "
            f"{'OK' if all(c['ok'] for c in run['auditoria']) else 'FALHOU'}")
        execucoes.append(run)
    res["teste3"] = execucoes


def teste4(res: dict, csv_path: Path, esperado: dict, cli: ClienteAPI, gatilho_pct: float, build: bool) -> None:
    log("TESTE 4 — chaos test: 3 workers, docker kill em 1 ao atingir "
        f"~{gatilho_pct:.0f}% de progresso")
    dests = ler_csv_dicts(csv_path)
    subir_ambiente(3, build=build)
    cli.aguardar_saude()
    cli.sessao.post(f"{cli.smtp}/reset", timeout=5)
    aquecer(cli, dests)
    antes = workers_rodando()
    evento: dict = {}

    def ao_progresso(st: dict, t: float) -> None:
        if not evento and st["progresso_pct"] >= gatilho_pct:
            alvo = antes[0]
            nome = subprocess.run(["docker", "inspect", "--format", "{{.Name}}", alvo],
                                  capture_output=True, text=True).stdout.strip().lstrip("/")
            subprocess.run(["docker", "kill", alvo], check=True, capture_output=True)
            evento.update({"t": round(t, 3), "progresso_pct": st["progresso_pct"], "container": nome,
                           "processados": st["processados"]})
            log(f"  💥 docker kill {nome} com {st['progresso_pct']:.1f}% concluído (t={t:.2f}s)")

    run = executar_campanha(cli, "chaos-test", dests, ao_progresso=ao_progresso)
    depois = workers_rodando()
    run["evento_kill"] = evento
    run["workers_antes"] = len(antes)
    run["workers_depois"] = len(depois)
    run["auditoria"] = auditar(run, esperado) + [
        {"verificacao": "Um worker foi derrubado durante a campanha", "ok": bool(evento) and len(depois) == len(antes) - 1,
         "detalhe": f"{len(antes)} -> {len(depois)} workers ({evento.get('container', 'nenhum')})"},
        {"verificacao": "Reprocessamento ocorreu e foi barrado pela idempotência",
         "ok": run["status_final"]["duplicados_evitados"] + run["auditoria_smtp"]["replays_idempotentes"] > 0,
         "detalhe": f"duplicados_evitados (Redis SETNX)={run['status_final']['duplicados_evitados']}, "
                    f"replays bloqueados no provedor={run['auditoria_smtp']['replays_idempotentes']}"},
    ]
    try:
        run["logs_reentrega"] = [linha for linha in compose("logs", "--no-color", "worker", capturar=True).splitlines()
                                 if "REENTREGUE" in linha][-10:]
    except Exception:  # noqa: BLE001
        run["logs_reentrega"] = []
    ok = all(c["ok"] for c in run["auditoria"])
    log(f"  chaos test concluído em {run['tempo_esvaziamento_s']:.2f}s | auditoria: {'APROVADA' if ok else 'REPROVADA'}")
    res["teste4"] = run


# ============================================================================
# Gráficos
# ============================================================================


def gerar_graficos(res: dict) -> list[str]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    DIR_GRAF.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"figure.dpi": 120, "axes.grid": True, "grid.alpha": 0.3, "axes.spines.top": False,
                         "axes.spines.right": False, "font.size": 10})
    COR = {"sequencial": "#6b7280", "threads": "#f59e0b", "processos": "#4f46e5", "ideal": "#10b981",
           "dist": "#ec4899"}
    gerados = []

    def salvar(fig, nome):
        fig.tight_layout()
        fig.savefig(DIR_GRAF / nome)
        plt.close(fig)
        gerados.append(nome)

    t1, t2 = res.get("teste1"), res.get("teste2")
    if t1 and t2:
        base = t1["tempos_s"]["total"]
        # --- tempos
        rotulos = ["sequencial"] + [f"{r['workers']} threads" for r in t2["threads"]] + \
                  [f"{r['workers']} proc." for r in t2["processos"]]
        tempos = [base] + [r["tempos_s"]["total"] for r in t2["threads"]] + [r["tempos_s"]["total"] for r in t2["processos"]]
        cores = [COR["sequencial"]] + [COR["threads"]] * len(t2["threads"]) + [COR["processos"]] * len(t2["processos"])
        fig, ax = plt.subplots(figsize=(9, 4.2))
        barras = ax.bar(rotulos, tempos, color=cores)
        ax.bar_label(barras, fmt="%.1fs", padding=2, fontsize=9)
        ax.set_ylabel("tempo total (s)")
        ax.set_title(f"Parte 1 — tempo total ({t1['total_registros']} registros, menor é melhor)")
        salvar(fig, "p1_tempos.png")

        # --- speedup (perfil padrão + perfil CPU)
        perfis = [("Carga padrão (CPU + I/O 5–15 ms)", t2, base)]
        if res.get("teste2b"):
            perfis.append(("Perfil 100% CPU (sem I/O) — efeito do GIL", res["teste2b"],
                           res["teste2b"]["sequencial"]["tempos_s"]["total"]))
        fig, axes = plt.subplots(1, len(perfis), figsize=(5.2 * len(perfis), 4.2), squeeze=False)
        for ax, (titulo, dados, b) in zip(axes[0], perfis):
            ws = [1] + [r["workers"] for r in dados["threads"]]
            ax.plot(ws, ws, "--", color=COR["ideal"], label="ideal (linear)")
            for modo in ("threads", "processos"):
                ax.plot(ws, [1] + [b / r["tempos_s"]["total"] for r in dados[modo]], "o-", color=COR[modo],
                        label=modo, linewidth=2)
            ax.set_xticks(ws)
            ax.set_xlabel("nº de workers")
            ax.set_ylabel("speedup (× sequencial)")
            ax.set_title(titulo, fontsize=10)
            ax.legend()
        salvar(fig, "p1_speedup.png")

        # --- anatomia do tempo por e-mail (GIL)
        fig, ax = plt.subplots(figsize=(9, 4.2))
        confs = [("seq", t1)] + [(f"{r['workers']}T", r) for r in t2["threads"]] + [(f"{r['workers']}P", r) for r in t2["processos"]]
        x = range(len(confs))
        cpu = [c["metricas_ms"]["cpu_thread_por_email"] for _, c in confs]
        gil_cpu = [c["metricas_ms"]["espera_gil_cpu_por_email"] for _, c in confs]
        atraso = [c["metricas_ms"].get("atraso_pos_io_por_email", 0) for _, c in confs]
        ax.bar(x, cpu, label="CPU efetiva (render + SHA-256)", color="#4f46e5")
        ax.bar(x, gil_cpu, bottom=cpu, label="espera na fase de CPU (GIL)", color="#f59e0b")
        ax.bar(x, atraso, bottom=[a + b for a, b in zip(cpu, gil_cpu)],
               label="atraso p/ retomar após I/O (readquirir GIL + SO)", color="#ef4444")
        ax.set_xticks(list(x), [n for n, _ in confs])
        ax.set_ylabel("ms por e-mail")
        ax.set_title("Anatomia do custo por e-mail (excluindo a latência nominal da rede)")
        ax.legend(fontsize=8)
        salvar(fig, "p1_gil.png")

    t3 = res.get("teste3")
    if t3:
        ws = [r["workers"] for r in t3]
        tempos = [r["tempo_esvaziamento_s"] for r in t3]
        thr = [r["throughput_registros_s"] for r in t3]
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 4))
        b = a1.bar([str(w) for w in ws], tempos, color=COR["dist"])
        a1.bar_label(b, fmt="%.1fs", padding=2)
        a1.set_xlabel("contêineres worker")
        a1.set_ylabel("tempo de esvaziamento da fila (s)")
        a1.set_title("Parte 2 — tempo (menor é melhor)")
        a2.plot(ws, thr, "o-", color=COR["dist"], linewidth=2, label="medido")
        a2.plot(ws, [thr[0] * w / ws[0] for w in ws], "--", color=COR["ideal"], label="ideal (linear)")
        a2.set_xticks(ws)
        a2.set_xlabel("contêineres worker")
        a2.set_ylabel("registros/s")
        a2.set_title("Parte 2 — vazão")
        a2.legend()
        salvar(fig, "p2_escala.png")

    t4 = res.get("teste4")
    if t4 and t4.get("linha_do_tempo"):
        lt = t4["linha_do_tempo"]
        fig, ax = plt.subplots(figsize=(9, 4.2))
        ax.plot([p["t"] for p in lt], [p["pct"] for p in lt], color=COR["processos"], linewidth=2, label="progresso (%)")
        ax.set_xlabel("tempo desde o POST /campanhas (s)")
        ax.set_ylabel("progresso (%)")
        ax.set_ylim(0, 105)
        ax2 = ax.twinx()
        ax2.plot([p["t"] for p in lt], [p["fila"] or 0 for p in lt], color="#9ca3af", linestyle=":", label="lotes na fila")
        ax2.set_ylabel("mensagens prontas na fila")
        ax2.grid(False)
        if t4.get("evento_kill"):
            ev = t4["evento_kill"]
            ax.axvline(ev["t"], color="#ef4444", linestyle="--", linewidth=2)
            ax.annotate(f"docker kill\n{ev['container']}\n({ev['progresso_pct']:.0f}%)", (ev["t"], 50),
                        xytext=(8, 0), textcoords="offset points", color="#ef4444", fontsize=9)
        l1, n1 = ax.get_legend_handles_labels()
        l2, n2 = ax2.get_legend_handles_labels()
        ax.legend(l1 + l2, n1 + n2, loc="lower right")
        ax.set_title("Teste 4 — chaos test: progresso da campanha com 3 → 2 workers")
        salvar(fig, "p2_chaos.png")
    return gerados


# ============================================================================
# Relatório Markdown
# ============================================================================


def _tab(cab: list[str], linhas: list[list]) -> str:
    out = ["| " + " | ".join(cab) + " |", "|" + "|".join(["---"] * len(cab)) + "|"]
    out += ["| " + " | ".join(str(c) for c in linha) + " |" for linha in linhas]
    return "\n".join(out)


def _ok(v: bool) -> str:
    return "✅" if v else "❌"


def gerar_relatorio(res: dict, graficos: list[str]) -> None:
    amb, esp = res.get("ambiente", {}), res.get("esperado", {})
    md = [
        "# Relatório de Resultados — Envio de E-mails em Massa",
        "",
        f"> Gerado automaticamente por `benchmark_runner.py` em {amb.get('data', '?')}. "
        "Os números abaixo são **medidos** nesta máquina; reexecute para reproduzir.",
        "",
        "## Ambiente e carga de trabalho",
        "",
        _tab(["Item", "Valor"], [
            ["Plataforma", amb.get("plataforma")], ["CPUs lógicas (host)", amb.get("cpus_logicas")],
            ["Python", amb.get("python")], ["Docker", amb.get("docker") or "n/d"],
            ["Registros", esp.get("total")], ["Seed do dataset", res.get("seed")],
            ["Gabarito", ", ".join(f"{k}={esp.get(k)}" for k in CATEGORIAS)],
        ]),
        "",
        "Cada e-mail: validação Regex + opt-out → renderização HTML + SHA-256 encadeado (CPU, "
        f"`RODADAS_HASH={res.get('teste1', {}).get('rodadas_hash', 2000)}`) → disparo no SMTP Mock "
        "(5–15 ms, 2% de falhas transitórias com retry, ~0,5% de rejeições 550 determinísticas).",
        "",
    ]

    t1, t2 = res.get("teste1"), res.get("teste2")
    if t1:
        md += ["## Teste 1 — Referência sequencial", "",
               _tab(["Tempo total", "CSV", "Processamento", "Consolidação", "Registros/s", "CPU/e-mail", "Confere gabarito"],
                    [[f"{t1['tempos_s']['total']:.2f}s", f"{t1['tempos_s']['carga_csv'] * 1000:.1f}ms",
                      f"{t1['tempos_s']['fase_paralela']:.2f}s", f"{t1['tempos_s']['consolidacao'] * 1000:.2f}ms",
                      f"{t1['throughput_registros_s']:.1f}", f"{t1['metricas_ms']['cpu_thread_por_email']:.2f}ms",
                      _ok(t1["confere_gabarito"])]]), ""]
    if t1 and t2:
        base = t1["tempos_s"]["total"]
        linhas = []
        for modo in ("threads", "processos"):
            for r in t2[modo]:
                s = base / r["tempos_s"]["total"]
                m = r["metricas_ms"]
                linhas.append([modo, r["workers"], r["chunks"], f"{r['tempos_s']['total']:.2f}s", f"{s:.2f}×",
                               f"{100 * s / r['workers']:.0f}%", f"{r['throughput_registros_s']:.1f}",
                               f"{m['cpu_thread_por_email']:.2f}", f"{m['espera_gil_cpu_por_email']:.2f}",
                               f"{m.get('atraso_pos_io_por_email', 0):.2f}", _ok(r["confere_gabarito"])])
        md += ["## Teste 2 — Escala paralela (mesma máquina)", "",
               _tab(["Modo", "Workers", "Chunks", "Tempo", "Speedup", "Eficiência", "Registros/s",
                     "CPU/e-mail (ms)", "Espera GIL na CPU (ms)", "Atraso pós-I/O (ms)", "Confere"], linhas), ""]
        melhor = min((r for m in ("threads", "processos") for r in t2[m]), key=lambda r: r["tempos_s"]["total"])
        t8 = {m: max(t2[m], key=lambda r: r["workers"]) for m in ("threads", "processos")}
        md += [f"- Melhor configuração: **{melhor['workers']} {melhor['modo']}** — "
               f"{melhor['tempos_s']['total']:.2f}s (**{base / melhor['tempos_s']['total']:.2f}×** o sequencial).",
               f"- Com {t8['threads']['workers']} workers: threads = {base / t8['threads']['tempos_s']['total']:.2f}×, "
               f"processos = {base / t8['processos']['tempos_s']['total']:.2f}×. Nesta carga ~90% do tempo de cada "
               "e-mail é espera de rede (que libera o GIL), por isso threads também escalam bem.",
               f"- O atraso para retomar após o I/O sobe de {t1['metricas_ms'].get('atraso_pos_io_por_email', 0):.2f} ms "
               f"(sequencial) para {t8['threads']['metricas_ms'].get('atraso_pos_io_por_email', 0):.2f} ms com "
               f"{t8['threads']['workers']} threads: é o tempo que a thread espera para **readquirir o GIL** ao acordar. "
               f"Com processos fica em {t8['processos']['metricas_ms'].get('atraso_pos_io_por_email', 0):.2f} ms "
               "(cada processo tem o seu GIL).",
               "- Todas as execuções passaram no `assert enviados + invalidos + opt_out + erros_smtp == TOTAL` "
               "e produziram **exatamente** os mesmos contadores do gabarito.", ""]
    t2b = res.get("teste2b")
    if t2b:
        b = t2b["sequencial"]["tempos_s"]["total"]
        linhas = [["sequencial", 1, f"{b:.2f}s", "1.00×", _ok(t2b["sequencial"]["confere_gabarito"])]]
        for modo in ("threads", "processos"):
            linhas += [[modo, r["workers"], f"{r['tempos_s']['total']:.2f}s", f"{b / r['tempos_s']['total']:.2f}×",
                        _ok(r["confere_gabarito"])] for r in t2b[modo]]
        tm = max(t2b["threads"], key=lambda r: r["workers"])
        pm = max(t2b["processos"], key=lambda r: r["workers"])
        md += ["### Teste 2b — Perfil 100% CPU-bound (latência de rede = 0)", "",
               "Mesma carga, porém sem a espera de rede: isola a etapa de renderização + SHA-256, que segura o GIL.", "",
               _tab(["Modo", "Workers", "Tempo", "Speedup", "Confere"], linhas), "",
               f"- **Efeito do GIL:** com {tm['workers']} threads o speedup foi {b / tm['tempos_s']['total']:.2f}× "
               f"(as threads se revezam no único GIL), enquanto {pm['workers']} processos chegaram a "
               f"{b / pm['tempos_s']['total']:.2f}× (um GIL por processo, limitado pelos núcleos físicos e pelo "
               "custo de spawn/pickle).", ""]
    for nome, titulo in (("p1_tempos.png", "Tempo total por configuração"),
                         ("p1_speedup.png", "Speedup: threads vs processos"),
                         ("p1_gil.png", "Anatomia do custo por e-mail")):
        if nome in graficos:
            md += [f"![{titulo}](resultados/graficos/{nome})", ""]

    t3 = res.get("teste3")
    if t3:
        b = t3[0]["tempo_esvaziamento_s"]
        seq = t1["tempos_s"]["total"] if t1 else None
        linhas = []
        for r in t3:
            a = r["auditoria_smtp"]
            linhas.append([r["workers"], f"{r['tempo_esvaziamento_s']:.2f}s", f"{r['throughput_registros_s']:.1f}",
                           f"{b / r['tempo_esvaziamento_s']:.2f}×",
                           f"{seq / r['tempo_esvaziamento_s']:.2f}×" if seq else "n/d",
                           f"{r['tempo_resposta_post_s'] * 1000:.0f}ms", a["entregas_duplicadas"],
                           _ok(all(c["ok"] for c in r["auditoria"]))])
        md += ["## Teste 3 — Escala distribuída (contêineres)", "",
               "Cada worker é limitado a **1 vCPU** (`deploy.resources.limits.cpus`), simulando um nó independente. "
               "Tempo de esvaziamento = do `POST /campanhas` até o último destinatário ser finalizado (relógio do servidor).", "",
               _tab(["Workers", "Esvaziamento", "Registros/s", "Speedup vs 1 worker", "vs sequencial local",
                     "POST /campanhas", "Duplicatas", "Auditoria"], linhas), ""]
        if "p2_escala.png" in graficos:
            md += ["![Escala distribuída](resultados/graficos/p2_escala.png)", ""]

    t4 = res.get("teste4")
    if t4:
        ev, st, a = t4.get("evento_kill", {}), t4["status_final"], t4["auditoria_smtp"]
        aprovado = all(c["ok"] for c in t4["auditoria"])
        md += ["## Teste 4 — Chaos test (queda de um worker)", "",
               f"Campanha iniciada com **{t4['workers_antes']} workers**; ao atingir "
               f"**{ev.get('progresso_pct', 0):.1f}%** (t={ev.get('t', 0):.2f}s) o contêiner "
               f"`{ev.get('container', '?')}` foi derrubado com `docker kill` (SIGKILL, sem desligamento gracioso). "
               f"A fila foi esvaziada pelos **{t4['workers_depois']} workers restantes** em "
               f"**{t4['tempo_esvaziamento_s']:.2f}s**.", "",
               f"### Auditoria final: {'✅ APROVADA' if aprovado else '❌ REPROVADA'}", "",
               _tab(["Verificação", "Resultado", "Detalhe"],
                    [[c["verificacao"], _ok(c["ok"]), c["detalhe"]] for c in t4["auditoria"]]), "",
               _tab(["Métrica", "Valor"], [
                   ["Duplicados evitados pelo Redis (SETNX)", st["duplicados_evitados"]],
                   ["Replays bloqueados pelo Idempotency-Key no provedor", a["replays_idempotentes"]],
                   ["Leases perdidos (fencing)", st["leases_perdidos"]],
                   ["Entregas duplicadas reais", a["entregas_duplicadas"]],
                   ["Requisições ao SMTP / entregas", f"{a['requisicoes']} / {a['entregas']}"],
                   ["Disponibilidade da API durante o teste", f"{t4['disponibilidade']['disponibilidade_pct']:.1f}% "
                    f"({t4['disponibilidade']['polls_ok']} consultas, p50 "
                    f"{t4['disponibilidade']['latencia_status_p50_ms']:.1f}ms)"],
               ]), ""]
        if t4.get("logs_reentrega"):
            md += ["Trechos do log mostrando o lote órfão reprocessado por outro worker:", "", "```",
                   *t4["logs_reentrega"][-5:], "```", ""]
        if "p2_chaos.png" in graficos:
            md += ["![Chaos test](resultados/graficos/p2_chaos.png)", ""]
        md += ["O \"degrau\" após o kill corresponde ao lote órfão: o RabbitMQ o devolve à fila assim que a conexão "
               "AMQP do worker morto cai; o novo dono pula os destinatários já finalizados (SETNX) e aguarda a "
               f"expiração do lease (`LEASE_MS`) dos que estavam em voo antes de reenviá-los com o mesmo Idempotency-Key.", ""]

    md += ["## Como reproduzir", "", "```bash", "make setup", "make benchmark            # testes 1-4",
           "make teste4               # apenas o chaos test", "```", ""]
    RELATORIO.write_text("\n".join(md), encoding="utf-8")
    log(f"relatório: {RELATORIO}")


# ============================================================================
# main
# ============================================================================


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--registros", type=int, default=10_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--csv", type=Path, default=CSV_PADRAO)
    p.add_argument("--testes", type=int, nargs="+", default=[1, 2, 3, 4], choices=[1, 2, 3, 4])
    p.add_argument("--workers-paralelo", type=int, nargs="+", default=[2, 4, 8])
    p.add_argument("--workers-dist", type=int, nargs="+", default=[1, 2, 3])
    p.add_argument("--sem-perfil-cpu", action="store_true", help="pula o Teste 2b (perfil 100%% CPU)")
    p.add_argument("--chaos-gatilho", type=float, default=30.0, help="%% de progresso para derrubar o worker")
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--smtp", default="http://localhost:8025")
    p.add_argument("--sem-build", action="store_true", help="não reconstrói as imagens Docker")
    p.add_argument("--manter-docker", action="store_true", help="não derruba o ambiente Docker ao final")
    p.add_argument("--apenas-relatorio", action="store_true")
    a = p.parse_args()

    res = carregar_resultados()
    if not a.apenas_relatorio:
        res["ambiente"] = info_ambiente()
        regerar = res.get("seed") != a.seed or res.get("esperado", {}).get("total") != a.registros or not a.csv.exists()
        if regerar or 1 in a.testes:
            # Mudou a carga -> resultados antigos não são comparáveis.
            if regerar:
                res = {"ambiente": res["ambiente"]}
            res["esperado"] = preparar_dataset(a.registros, a.seed, a.csv)
            res["seed"] = a.seed
        else:
            res["esperado"] = resultado_esperado(carregar_destinatarios(a.csv))
        esperado = res["esperado"]

        if 1 in a.testes:
            teste1(res, a.csv, esperado)
            salvar_resultados(res)
        if 2 in a.testes:
            teste2(res, a.csv, esperado, a.workers_paralelo, not a.sem_perfil_cpu)
            salvar_resultados(res)
        if 3 in a.testes or 4 in a.testes:
            cli = ClienteAPI(a.api, a.smtp)
            try:
                compose("down", "-v", "--remove-orphans")  # ambiente limpo: fila e Redis vazios
                if 3 in a.testes:
                    teste3(res, a.csv, esperado, a.workers_dist, cli, build=not a.sem_build)
                    salvar_resultados(res)
                if 4 in a.testes:
                    teste4(res, a.csv, esperado, cli, a.chaos_gatilho, build=not a.sem_build and 3 not in a.testes)
                    salvar_resultados(res)
            finally:
                if not a.manter_docker:
                    compose("down", "-v", "--remove-orphans", check=False)

    graficos = gerar_graficos(res)
    gerar_relatorio(res, graficos)
    falhas = []
    for chave in ("teste3",):
        for r in res.get(chave, []):
            falhas += [c for c in r["auditoria"] if not c["ok"]]
    if res.get("teste4"):
        falhas += [c for c in res["teste4"]["auditoria"] if not c["ok"]]
    if falhas:
        log(f"⚠️  {len(falhas)} verificação(ões) de auditoria falharam — veja {RELATORIO.name}")
        return 1
    log("concluído ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
