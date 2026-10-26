# syntax=docker/dockerfile:1
# Contexto de build = raiz do repositório (para compartilhar o pacote common/).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY parte2_distribuido/worker_service/requirements.txt .
RUN pip install -r requirements.txt

COPY common/ ./common/
COPY parte2_distribuido/worker_service/*.py ./worker_service/

RUN useradd --create-home --uid 10001 app
USER app

# O worker não expõe porta: o healthcheck verifica se o event loop atualizou o heartbeat.
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=6 \
  CMD python -c "import time,sys; sys.exit(0 if time.time()-float(open('/tmp/worker_heartbeat').read())<20 else 1)" || exit 1

CMD ["python", "worker_service/worker.py"]
