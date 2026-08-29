"""
Validações de Qualidade de Dados

Executado após a carga da camada Silver:
    - Verificação de duplicidade
    - Detecção de valores ausentes
    - Validação de chaves de relacionamento (FKs entre bases)
    - Consistência entre tabelas (ex.: percentuais entre 0 e 100)

Falhas críticas interrompem o pipeline (via exceção); falhas não
críticas apenas geram um aviso no log.

Execução:
    spark-submit -m src.quality.data_quality_checks
"""
from __future__ import annotations

from dataclasses import dataclass, field

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config import settings


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str
    critical: bool = True
    metrics: dict = field(default_factory=dict)


def check_duplicidade(df: DataFrame, key_cols: list[str]) -> CheckResult:
    total = df.count()
    distintos = df.select(*key_cols).distinct().count()
    duplicados = total - distintos
    return CheckResult(
        name="verificacao_duplicidade",
        passed=duplicados == 0,
        details=f"{duplicados} registros duplicados em {key_cols} (de {total} totais)",
        metrics={"total": total, "duplicados": duplicados},
    )


def check_valores_ausentes(df: DataFrame, cols: list[str], limite_pct: float = 5.0) -> CheckResult:
    total = df.count()
    detalhes = {}
    falhou = False
    for c in cols:
        nulos = df.filter(F.col(c).isNull()).count()
        pct = round((nulos / total * 100) if total else 0, 2)
        detalhes[c] = pct
        if pct > limite_pct:
            falhou = True
    return CheckResult(
        name="deteccao_valores_ausentes",
        passed=not falhou,
        details=f"% de nulos por coluna (limite {limite_pct}%): {detalhes}",
        critical=False,  # ausências são tratadas com flag, não bloqueiam o pipeline
        metrics=detalhes,
    )


def check_chave_relacionamento(df_filho: DataFrame, df_pai: DataFrame, key_cols: list[str]) -> CheckResult:
    """Garante que toda chave estrangeira em df_filho existe em df_pai."""
    orfaos = df_filho.select(*key_cols).distinct().join(
        df_pai.select(*key_cols).distinct(), on=key_cols, how="left_anti"
    )
    qtd_orfaos = orfaos.count()
    return CheckResult(
        name="validacao_chave_relacionamento",
        passed=qtd_orfaos == 0,
        details=f"{qtd_orfaos} registros com chave {key_cols} sem correspondência na dimensão",
        metrics={"orfaos": qtd_orfaos},
    )


def check_consistencia_percentuais(df: DataFrame, col: str, tolerancia: float = 1e-6) -> CheckResult:
    """Percentuais devem estar em [0, 100]. Uma pequena tolerância
    absorve erro de arredondamento de ponto flutuante em agregações
    (ex.: 100.000000000000014 resultante de uma divisão SUM/SUM)."""
    invalidos = df.filter((F.col(col) < -tolerancia) | (F.col(col) > 100 + tolerancia)).count()
    return CheckResult(
        name=f"consistencia_{col}",
        passed=invalidos == 0,
        details=f"{invalidos} registros com '{col}' fora do intervalo [0, 100]",
        metrics={"invalidos": invalidos},
    )


def run_all(spark: SparkSession) -> list[CheckResult]:
    silver = spark.read.parquet(f"gs://{settings.bucket_silver}/indicador_alfabetizacao/")
    municipio = spark.read.parquet(f"gs://{settings.bucket_bronze}/municipio/")

    results = [
        check_duplicidade(silver, ["id_municipio", "ano"]),
        check_valores_ausentes(silver, ["proficiencia_media", "percentual_alfabetizado"]),
        check_chave_relacionamento(silver, municipio, ["id_municipio"]),
        check_consistencia_percentuais(silver, "percentual_alfabetizado"),
    ]

    for r in results:
        status = "OK" if r.passed else "FALHOU"
        print(f"[quality] {r.name}: {status} — {r.details}")
        if not r.passed and r.critical:
            logger.error(f"Checagem crítica falhou: {r.name} — {r.details}")

    criticas_falhas = [r for r in results if r.critical and not r.passed]
    if criticas_falhas:
        nomes = ", ".join(r.name for r in criticas_falhas)
        raise RuntimeError(f"Pipeline interrompido — checagens críticas falharam: {nomes}")

    return results


def main() -> None:
    spark = (
        SparkSession.builder.appName("quality_checks")
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.21")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", settings.gcp_service_account_key)
        .getOrCreate()
    )
    run_all(spark)
    spark.stop()


if __name__ == "__main__":
    main()
