# syntax=docker/dockerfile:1
# Contexto de build = raiz do repositório (para compartilhar o pacote common/).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app

# 1) dependências primeiro -> camada cacheada enquanto requirements.txt não mudar
COPY parte2_distribuido/smtp_mock/requirements.txt .
RUN pip install -r requirements.txt

# 2) código
COPY common/ ./common/
COPY parte2_distribuido/smtp_mock/main.py ./smtp_mock/main.py

RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8025
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:8025/health', timeout=2)" || exit 1

# 1 único processo de propósito: o estado de auditoria vive em memória.
CMD ["uvicorn", "smtp_mock.main:app", "--host", "0.0.0.0", "--port", "8025", "--no-access-log"]
