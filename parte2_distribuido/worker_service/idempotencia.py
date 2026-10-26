"""Idempotência com consistência FORTE no Redis (fonte da verdade).

Máquina de estados por chave ``idem:{sha256(campanha_id:email)}``::

    (inexistente) --SET NX PX lease--> PROC:<worker>:<token>   (lease com TTL)
    PROC:<dono>   --finalizar (dono)--> ENVIADO | ERRO_SMTP     (persistente)
    (inexistente) --finalizar s/ dono-> INVALIDO | OPT_OUT
    PROC:<dono>   --lease expira-----> (inexistente)            (dono morreu -> outro reassume)

Todas as transições são scripts Lua: o Redis executa cada script de forma
ATÔMICA (single-thread), então "checar + gravar + incrementar o contador" nunca
é intercalado por outro worker. Isso garante:
  * cada destinatário é contabilizado EXATAMENTE uma vez nos contadores;
  * um lote reprocessado (após queda de um worker) pula quem já foi finalizado;
  * fencing: um worker que perdeu o lease não consegue sobrescrever o estado.
"""

from __future__ import annotations

from redis import asyncio as aioredis

# KEYS[1]=idem ; ARGV[1]=token ; ARGV[2]=lease_ms
# Retorno: "OK" (lease obtido) | valor atual (outro dono/terminal) | false (expirou entre SET e GET)
LUA_RECLAMAR = """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return 'OK'
end
return redis.call('GET', KEYS[1])
"""

# KEYS[1]=idem KEYS[2]=stats ; ARGV[1]=token ('' = sem lease) ARGV[2]=estado ARGV[3]=campo
#                               ARGV[4]=latencia_ms ARGV[5]=agora ARGV[6]=ttl_s
# Retorno: 1 finalizado | 0 já existia (duplicado evitado) | -1 lease perdido
LUA_FINALIZAR = """
local atual = redis.call('GET', KEYS[1])
if ARGV[1] == '' then
  if atual then
    redis.call('HINCRBY', KEYS[2], 'duplicados_evitados', 1)
    return 0
  end
elseif atual ~= ARGV[1] then
  redis.call('HINCRBY', KEYS[2], 'leases_perdidos', 1)
  return -1
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[6])
redis.call('HINCRBY', KEYS[2], ARGV[3], 1)
if tonumber(ARGV[4]) > 0 then
  redis.call('HINCRBYFLOAT', KEYS[2], 'latencia_total_ms', ARGV[4])
end
redis.call('HSET', KEYS[2], 'atualizada_em', ARGV[5])
return 1
"""

# Libera o lease (apenas se ainda for o dono) para que outro worker possa reassumir já.
LUA_LIBERAR = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class Idempotencia:
    def __init__(self, redis: aioredis.Redis, lease_ms: int, ttl_terminal_s: int) -> None:
        self.redis = redis
        self.lease_ms = lease_ms
        self.ttl_terminal_s = ttl_terminal_s
        self._reclamar = redis.register_script(LUA_RECLAMAR)
        self._finalizar = redis.register_script(LUA_FINALIZAR)
        self._liberar = redis.register_script(LUA_LIBERAR)

    async def reclamar(self, k_idem: str, token: str) -> str | None:
        r = await self._reclamar(keys=[k_idem], args=[token, self.lease_ms])
        return r.decode() if isinstance(r, bytes) else r

    async def finalizar(self, k_idem: str, k_stats: str, token: str, estado: str, campo: str,
                        latencia_ms: float, agora: float) -> int:
        return int(await self._finalizar(
            keys=[k_idem, k_stats],
            args=[token, estado, campo, f"{latencia_ms:.3f}", f"{agora:.6f}", self.ttl_terminal_s],
        ))

    async def liberar(self, k_idem: str, token: str) -> None:
        await self._liberar(keys=[k_idem], args=[token])
