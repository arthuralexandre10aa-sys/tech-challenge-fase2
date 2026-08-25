"""
Testes unitários das regras de qualidade de dados.

Execução:
    pytest tests/ -v
"""
import pytest
from pyspark.sql import SparkSession

from src.quality.data_quality_checks import (
    check_chave_relacionamento,
    check_consistencia_percentuais,
    check_duplicidade,
    check_valores_ausentes,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_quality").getOrCreate()


def test_check_duplicidade_detecta_duplicados(spark):
    df = spark.createDataFrame([("SP", 2024), ("SP", 2024), ("RJ", 2024)], ["sigla_uf", "ano"])
    result = check_duplicidade(df, ["sigla_uf", "ano"])
    assert result.passed is False
    assert result.metrics["duplicados"] == 1


def test_check_duplicidade_sem_duplicados(spark):
    df = spark.createDataFrame([("SP", 2024), ("RJ", 2024)], ["sigla_uf", "ano"])
    result = check_duplicidade(df, ["sigla_uf", "ano"])
    assert result.passed is True


def test_check_valores_ausentes_dentro_do_limite(spark):
    df = spark.createDataFrame([(1.0,), (2.0,), (None,)], ["percentual_alfabetizado"])
    result = check_valores_ausentes(df, ["percentual_alfabetizado"], limite_pct=50)
    assert result.passed is True


def test_check_chave_relacionamento_encontra_orfaos(spark):
    filho = spark.createDataFrame([("001",), ("002",)], ["id_municipio"])
    pai = spark.createDataFrame([("001",)], ["id_municipio"])
    result = check_chave_relacionamento(filho, pai, ["id_municipio"])
    assert result.passed is False
    assert result.metrics["orfaos"] == 1


def test_check_consistencia_percentuais_fora_do_intervalo(spark):
    df = spark.createDataFrame([(101.0,), (50.0,), (-5.0,)], ["percentual_alfabetizado"])
    result = check_consistencia_percentuais(df, "percentual_alfabetizado")
    assert result.passed is False
    assert result.metrics["invalidos"] == 2
