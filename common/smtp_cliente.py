"""Clientes SMTP usados pela Parte 1 (síncronos, um por chunk/worker).

* ``SMTPSimulado``: in-process. A latência de rede é simulada com ``time.sleep``,
  que — assim como uma chamada real de socket — LIBERA o GIL. Não exige nenhum
  serviço externo, garantindo benchmarks reprodutíveis.
* ``SMTPHttp``: faz um POST real para o ``smtp_mock`` da Parte 2
  (``--smtp-url http://localhost:8025``), exercitando a pilha TCP/HTTP de verdade.

Ambos seguem o mesmo contrato e a mesma política de falhas:
  - falha permanente determinística (SMTP 550)  -> ``ErroPermanente`` (sem retry)
  - falha transitória aleatória (SMTP 451/503)  -> ``ErroTransitorio`` (com retry)
"""

from __future__ import annotations

import random
import time

from common.carga import falha_permanente


class ErroSMTP(Exception):
    pass


class ErroTransitorio(ErroSMTP):
    pass


class ErroPermanente(ErroSMTP):
    pass


class SMTPSimulado:
    def __init__(
        self,
        latencia_min_ms: float = 5.0,
        latencia_max_ms: float = 15.0,
        taxa_falha_transitoria: float = 0.02,
        seed: int | None = None,
    ) -> None:
        self.lat_min = latencia_min_ms / 1000
        self.lat_max = latencia_max_ms / 1000
        self.taxa = taxa_falha_transitoria
        self.rng = random.Random(seed)  # RNG local -> thread-safe sem lock
        # Atraso acumulado entre o fim nominal do sleep e a retomada efetiva da
        # thread. Ao acordar, a thread precisa READQUIRIR o GIL: com várias threads
        # disputando CPU, é aqui que a espera pelo GIL aparece (+ folga do timer do SO).
        self.atraso_pos_io = 0.0

    def enviar(self, campanha_id: str, para: str, assunto: str, html: str,
               rastreio: str, idem_key: str) -> None:
        if self.lat_max > 0:
            nominal = self.rng.uniform(self.lat_min, self.lat_max)
            t0 = time.perf_counter()
            time.sleep(nominal)  # I/O: libera o GIL
            self.atraso_pos_io += max(0.0, time.perf_counter() - t0 - nominal)
        if falha_permanente(para):
            raise ErroPermanente("550 mailbox unavailable")
        if self.rng.random() < self.taxa:
            raise ErroTransitorio("451 try again later")

    def fechar(self) -> None:
        pass


class SMTPHttp:
    def __init__(self, base_url: str, timeout_s: float = 2.0) -> None:
        import requests  # import tardio: só exigido no modo HTTP

        self._requests = requests
        self.url = base_url.rstrip("/") + "/enviar"
        self.timeout = timeout_s
        self.sessao = requests.Session()  # keep-alive; uma sessão por worker

    def enviar(self, campanha_id: str, para: str, assunto: str, html: str,
               rastreio: str, idem_key: str) -> None:
        try:
            r = self.sessao.post(
                self.url,
                json={"campanha_id": campanha_id, "para": para, "assunto": assunto,
                      "html": html, "rastreio": rastreio},
                headers={"Idempotency-Key": idem_key},
                timeout=self.timeout,
            )
        except self._requests.RequestException as exc:
            raise ErroTransitorio(str(exc)) from exc
        if r.status_code == 200:
            return
        if r.status_code in (400, 422):
            raise ErroPermanente(r.text)
        raise ErroTransitorio(f"HTTP {r.status_code}")

    def fechar(self) -> None:
        self.sessao.close()


def enviar_com_retry(cliente, *args, max_tentativas: int = 4, backoff_base_s: float = 0.002,
                     rng: random.Random | None = None) -> tuple[float, int]:
    """Exponential backoff com jitter. Retorna (latência_da_tentativa_ok, retries).

    Propaga ``ErroPermanente`` imediatamente e ``ErroTransitorio`` após esgotar as tentativas.
    """
    rng = rng or random
    for tentativa in range(1, max_tentativas + 1):
        t0 = time.perf_counter()
        try:
            cliente.enviar(*args)
            return time.perf_counter() - t0, tentativa - 1
        except ErroTransitorio:
            if tentativa == max_tentativas:
                raise
            espera = backoff_base_s * (2 ** (tentativa - 1))
            time.sleep(espera + rng.uniform(0, espera))
    raise AssertionError("inalcançável")
