SHELL := /bin/bash
PYTHON := .venv/bin/python
REGISTROS ?= 10000
SEED ?= 42

.PHONY: help setup dataset test teste1 teste2 teste3 teste4 benchmark docker-up docker-down docker-logs clean

help:
	@echo "=========================================================================="
	@echo " Sistema de Envio de E-mails em Massa (Paralelo & Distribuído)"
	@echo "=========================================================================="
	@echo " Comandos disponíveis:"
	@echo "   make setup       - Cria o ambiente virtual e instala dependências"
	@echo "   make dataset     - Gera dataset/destinatarios.csv (padrão: 10.000 reg)"
	@echo "   make test        - Executa a suíte de testes unitários com pytest"
	@echo "   make teste1      - Executa o Teste 1 (Referência Sequencial)"
	@echo "   make teste2      - Executa o Teste 2 (Escala Paralela: Threads & Processos)"
	@echo "   make teste3      - Executa o Teste 3 (Escala Distribuída no Docker: 1, 2, 3 workers)"
	@echo "   make teste4      - Executa o Teste 4 (Chaos Test: Simulação de Falha com kill)"
	@echo "   make benchmark   - Executa Bateria Completa (Testes 1 a 4) + Relatório e Gráficos"
	@echo "   make docker-up   - Sobe os contêineres em background (2 workers)"
	@echo "   make docker-down - Desliga e limpa os contêineres e volumes"
	@echo "   make docker-logs - Exibe os logs dos contêineres"
	@echo "   make clean       - Limpa arquivos temporários, caches e resultados"
	@echo "=========================================================================="

setup:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt

dataset:
	@mkdir -p dataset
	$(PYTHON) -m dataset.gerador_carga --registros $(REGISTROS) --seed $(SEED) --saida dataset/destinatarios.csv

test:
	$(PYTHON) -m pytest -v tests/

teste1:
	@mkdir -p dataset resultados
	@test -f dataset/destinatarios.csv || $(MAKE) dataset
	$(PYTHON) benchmark_runner.py --testes 1 --registros $(REGISTROS) --seed $(SEED)

teste2:
	@mkdir -p dataset resultados
	@test -f dataset/destinatarios.csv || $(MAKE) dataset
	$(PYTHON) benchmark_runner.py --testes 2 --registros $(REGISTROS) --seed $(SEED)

teste3:
	@mkdir -p dataset resultados
	@test -f dataset/destinatarios.csv || $(MAKE) dataset
	$(PYTHON) benchmark_runner.py --testes 3 --registros $(REGISTROS) --seed $(SEED)

teste4:
	@mkdir -p dataset resultados
	@test -f dataset/destinatarios.csv || $(MAKE) dataset
	$(PYTHON) benchmark_runner.py --testes 4 --registros $(REGISTROS) --seed $(SEED)

benchmark:
	@mkdir -p dataset resultados
	$(PYTHON) benchmark_runner.py --testes 1 2 3 4 --registros $(REGISTROS) --seed $(SEED)

docker-up:
	docker compose -f parte2_distribuido/docker-compose.yml up -d --build --wait --scale worker=2

docker-down:
	docker compose -f parte2_distribuido/docker-compose.yml down -v --remove-orphans

docker-logs:
	docker compose -f parte2_distribuido/docker-compose.yml logs -f

clean:
	rm -rf .pytest_cache __pycache__ */__pycache__ */*/__pycache__ *.prof resultados/profiles/*.prof
	rm -f /tmp/smoke.csv
