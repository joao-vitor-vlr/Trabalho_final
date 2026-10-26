"""Carga de trabalho padronizada de um envio de e-mail.

Etapas por destinatário:
  1. Validação (Regex + opt-out)              -> barata
  2. Renderização do HTML + hash SHA-256       -> CPU-bound (segura o GIL)
  3. Disparo via rede (SMTP Mock)              -> I/O-bound (libera o GIL)

As etapas 1 e 2 vivem aqui. A etapa 3 tem duas implementações:
  - ``common/smtp_simulado.py`` (Parte 1: in-process ou HTTP)
  - ``parte2_distribuido/worker_service`` (Parte 2: aiohttp assíncrono)
"""

from __future__ import annotations

import hashlib
import html
import os
import re
from typing import NamedTuple

# ---------------------------------------------------------------------------
# Modelo de dados
# ---------------------------------------------------------------------------


class Destinatario(NamedTuple):
    """Registro imutável e leve (barato de serializar com pickle entre processos)."""

    id: int
    nome: str
    email: str
    segmento: str
    oferta_personalizada: str
    opt_out: bool


CAMPOS_CSV = ("id", "nome", "email", "segmento", "oferta_personalizada", "opt_out")

# ---------------------------------------------------------------------------
# Etapa 1 — Validação
# ---------------------------------------------------------------------------

# - sem ponto no início da parte local e sem ".." em lugar nenhum
# - parte local: letras, dígitos e . _ % + -
# - domínio: rótulos alfanuméricos/hífen separados por ponto + TLD com >= 2 letras
EMAIL_REGEX = re.compile(
    r"^(?!\.)(?!.*\.\.)[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$"
)


def email_valido(email: str) -> bool:
    return bool(email) and len(email) <= 254 and EMAIL_REGEX.match(email) is not None


def classificar(d: Destinatario) -> str | None:
    """Retorna o motivo de descarte ('invalidos' | 'opt_out') ou None se deve ser enviado.

    A ordem importa e é a mesma em todas as arquiteturas: sintaxe inválida tem
    precedência sobre opt-out.
    """
    if not email_valido(d.email):
        return "invalidos"
    if d.opt_out:
        return "opt_out"
    return None


# ---------------------------------------------------------------------------
# Etapa 2 — Renderização (CPU-bound)
# ---------------------------------------------------------------------------

# Fator de custo de CPU: quantas rodadas de SHA-256 encadeado são aplicadas ao
# token de rastreio (key stretching -> token não adivinhável). Como cada rodada
# opera sobre 32 bytes (< 2047 bytes), o hashlib NÃO libera o GIL, tornando esta
# etapa genuinamente CPU-bound em Python puro. Ajustável por variável de ambiente.
RODADAS_HASH_PADRAO = int(os.getenv("RODADAS_HASH", "2000"))

TEMPLATE_PADRAO = """<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="utf-8"><title>{{assunto}}</title></head>
<body style="font-family:Arial,sans-serif;background:#f4f6fb;padding:24px">
  <table width="600" align="center" style="background:#ffffff;border-radius:12px;padding:32px">
    <tr><td>
      <h1 style="color:#4f46e5">Olá, {{nome}}!</h1>
      <p>Separamos uma oferta exclusiva para o segmento <strong>{{segmento}}</strong>:</p>
      <p style="font-size:20px;color:#111827"><strong>{{oferta}}</strong></p>
      <p><a href="https://campanhas.exemplo.com/c/{{rastreio}}"
            style="background:#4f46e5;color:#fff;padding:12px 24px;border-radius:8px;text-decoration:none">
         Aproveitar agora</a></p>
      <p style="font-size:11px;color:#6b7280">
        Cliente #{{id}} &middot; Para não receber mais, acesse
        https://campanhas.exemplo.com/optout/{{rastreio}}
      </p>
      <img src="https://campanhas.exemplo.com/px/{{rastreio}}.gif" width="1" height="1" alt="">
    </td></tr>
  </table>
</body>
</html>"""

_PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")


def gerar_hash_rastreio(campanha_id: str, d: Destinatario, rodadas: int = RODADAS_HASH_PADRAO) -> str:
    """Token SHA-256 único por (campanha, destinatário), fortalecido por N rodadas."""
    h = hashlib.sha256(f"{campanha_id}:{d.id}:{d.email}".encode()).digest()
    for _ in range(rodadas):
        h = hashlib.sha256(h).digest()
    return h.hex()


def renderizar_html(
    template: str,
    d: Destinatario,
    campanha_id: str,
    assunto: str = "Oferta exclusiva",
    rodadas: int = RODADAS_HASH_PADRAO,
) -> tuple[str, str]:
    """Substitui as variáveis do template e devolve (html, token_rastreio)."""
    rastreio = gerar_hash_rastreio(campanha_id, d, rodadas)
    contexto = {
        "assunto": html.escape(assunto),
        "nome": html.escape(d.nome.split(" ")[0].title()),
        "segmento": html.escape(d.segmento),
        "oferta": html.escape(d.oferta_personalizada),
        "rastreio": rastreio,
        "id": str(d.id),
    }
    corpo = _PLACEHOLDER.sub(lambda m: contexto.get(m.group(1), ""), template)
    return corpo, rastreio


# ---------------------------------------------------------------------------
# Regras compartilhadas de rede / idempotência
# ---------------------------------------------------------------------------


def falha_permanente(email: str) -> bool:
    """Regra DETERMINÍSTICA do provedor simulado: ~0,5% das caixas "não existem" (SMTP 550).

    Por ser determinística (depende só do e-mail), todas as arquiteturas chegam ao
    mesmo número de ``erros_smtp`` — o que permite comparar a CORREÇÃO entre elas.
    """
    return int(hashlib.sha256(email.lower().encode()).hexdigest()[:8], 16) % 200 == 0


def chave_idempotencia(campanha_id: str, email: str) -> str:
    """idempotency_key = sha256(f"{campanha_id}:{email}")."""
    return hashlib.sha256(f"{campanha_id}:{email}".encode()).hexdigest()


def resultado_esperado(destinatarios: list[Destinatario]) -> dict[str, int]:
    """Gabarito usado pelas auditorias: o que um sistema CORRETO deve produzir."""
    esperado = {"enviados": 0, "invalidos": 0, "opt_out": 0, "erros_smtp": 0}
    for d in destinatarios:
        motivo = classificar(d)
        if motivo:
            esperado[motivo] += 1
        elif falha_permanente(d.email):
            esperado["erros_smtp"] += 1
        else:
            esperado["enviados"] += 1
    esperado["total"] = len(destinatarios)
    return esperado
