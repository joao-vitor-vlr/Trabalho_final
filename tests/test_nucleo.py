"""Testes unitários e de integração para validação de regras de negócio e concorrência."""

import pytest
from common.carga import (
    Destinatario,
    email_valido,
    classificar,
    gerar_hash_rastreio,
    renderizar_html,
    falha_permanente,
    chave_idempotencia,
    resultado_esperado,
    TEMPLATE_PADRAO
)
from parte1_paralelo.nucleo import dividir_em_chunks, consolidar, verificar_correcao
from collections import Counter


def test_validacao_sintaxe_email():
    validos = [
        "usuario@dominio.com",
        "nome.sobrenome@empresa.com.br",
        "user+tag@gmail.com",
        "contato123@servico.org",
    ]
    invalidos = [
        "sem_arroba.com",
        "dois@@arrobas.com",
        "sem_dominio@",
        "@sem_usuario.com",
        "usuario@dominio",  # sem TLD
        "usuario com espaco@dominio.com",
        ".comeca.com.ponto@dominio.com",
        "pontos..consecutivos@dominio.com",
        "usuario@dominio.c",  # TLD < 2 caracteres
        "",
    ]
    for email in validos:
        assert email_valido(email) is True, f"Deveria ser válido: {email}"
    for email in invalidos:
        assert email_valido(email) is False, f"Deveria ser inválido: {email}"


def test_classificacao_destinatario():
    # Destinatário válido normal
    d_ok = Destinatario(1, "Fulano Silva", "fulano@empresa.com", "varejo", "10% OFF", False)
    assert classificar(d_ok) is None

    # Destinatário com opt-out
    d_opt = Destinatario(2, "Ciclano Santos", "ciclano@empresa.com", "varejo", "10% OFF", True)
    assert classificar(d_opt) == "opt_out"

    # Destinatário com sintaxe inválida
    d_inv = Destinatario(3, "Beltrano", "invalido@@email.com", "varejo", "10% OFF", False)
    assert classificar(d_inv) == "invalidos"

    # Precedência: se inválido e opt-out, deve cair em inválidos primeiro
    d_ambos = Destinatario(4, "Duplo", "invalido@", "varejo", "10% OFF", True)
    assert classificar(d_ambos) == "invalidos"


def test_renderizacao_html_e_hash():
    d = Destinatario(10, "Maria Oliveira", "maria@teste.com", "tecnologia", "Notebook 15%", False)
    corpo, rastreio = renderizar_html(TEMPLATE_PADRAO, d, "camp-123", assunto="Super Promoção", rodadas=50)

    # Verifica integridade do rastreio
    assert len(rastreio) == 64  # SHA-256 hex
    assert rastreio == gerar_hash_rastreio("camp-123", d, rodadas=50)

    # Verifica variáveis substituídas no template
    assert "Maria" in corpo
    assert "tecnologia" in corpo
    assert "Notebook 15%" in corpo
    assert rastreio in corpo
    assert "Cliente #10" in corpo


def test_divisao_chunks_equilibrada():
    itens = list(range(10))
    chunks = dividir_em_chunks(itens, 3)
    assert len(chunks) == 3
    # 10 divididos em 3 chunks deve ser [4, 3, 3]
    tamanhos = [len(c) for c in chunks]
    assert tamanhos == [4, 3, 3]
    assert sum(tamanhos) == 10
    # Valida que todos os itens originais estão presentes e sem duplicação
    unidos = [x for c in chunks for x in c]
    assert unidos == itens

    # Edge cases
    assert dividir_em_chunks([], 5) == []
    assert len(dividir_em_chunks(list(range(3)), 5)) == 3


def test_consolidacao_e_verificacao_correcao():
    c1 = Counter({"enviados": 45, "invalidos": 2, "opt_out": 3, "erros_smtp": 0})
    c2 = Counter({"enviados": 48, "invalidos": 1, "opt_out": 0, "erros_smtp": 1})
    total = consolidar([c1, c2])

    assert total["enviados"] == 93
    assert total["invalidos"] == 3
    assert total["opt_out"] == 3
    assert total["erros_smtp"] == 1

    # Total de registros 100
    verificar_correcao(total, 100)

    # Se a soma não bater, deve disparar AssertionError
    with pytest.raises(AssertionError):
        verificar_correcao(total, 101)


def test_chave_idempotencia_determinismo():
    key1 = chave_idempotencia("camp-01", "cliente@teste.com")
    key2 = chave_idempotencia("camp-01", "cliente@teste.com")
    key3 = chave_idempotencia("camp-02", "cliente@teste.com")

    assert key1 == key2
    assert key1 != key3
    assert len(key1) == 64
