# syntax=docker/dockerfile:1
# Contexto de build = raiz do repositório (para compartilhar o pacote common/).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY parte2_distribuido/api_service/requirements.txt .
RUN pip install -r requirements.txt

COPY common/ ./common/
COPY parte2_distribuido/api_service/main.py ./api_service/main.py

RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=3s --retries=12 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)" || exit 1

CMD ["uvicorn", "api_service.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
