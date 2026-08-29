"""
Camada Silver — Limpeza, Padronização e Integração

Lê os dados brutos da camada Bronze, aplica:
    1. Limpeza (remoção de duplicidade)
    2. Tratamento de valores ausentes
    3. Padronização de nomes de colunas e tipos
    4. Normalização de chaves (id_municipio, sigla_uf, ano)
    5. Integração entre as bases (join município + UF + indicador + metas)

E grava o resultado consolidado em Parquet, particionado por ano/UF.

Execução:
    spark-submit -m src.silver.transform_silver
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

from src.utils.config import settings
from src.utils.observability import track_job


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver_alfabetizacao")
        # conector do GCS: permite ao Spark ler/escrever em caminhos gs://
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.21")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", settings.gcp_service_account_key)
        .config("spark.sql.shuffle.partitions", "8")  # dataset pequeno/médio: evita overhead
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, table_key: str) -> DataFrame:
    """Lê a camada Bronze de uma tabela e mantém apenas a ingestão mais
    recente (a Bronze preserva o histórico completo de todas as datas
    de ingestão em subpastas dt_ingestao=..., propositalmente — aqui
    filtramos para não duplicar registros ao processar a Silver)."""
    df = spark.read.parquet(f"gs://{settings.bucket_bronze}/{table_key}/")
    if "dt_ingestao" in df.columns:
        ultima_ingestao = df.select(F.max("dt_ingestao")).first()[0]
        df = df.filter(F.col("dt_ingestao") == ultima_ingestao)
    return df


def _normalizar_chaves(df: DataFrame) -> DataFrame:
    if "id_municipio" in df.columns:
        df = df.withColumn("id_municipio", F.col("id_municipio").cast(StringType()))
    if "sigla_uf" in df.columns:
        df = df.withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
    if "ano" in df.columns:
        df = df.withColumn("ano", F.col("ano").cast(IntegerType()))
    return df


def _limpar(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates()
    key_cols = [c for c in ["id_municipio", "ano"] if c in df.columns]
    if key_cols:
        df = df.dropna(how="all", subset=key_cols)
    return df


def _tratar_ausentes(df: DataFrame, numeric_cols: list[str]) -> DataFrame:
    """Sinaliza métricas ausentes numa coluna de flag, em vez de imputar
    valores sem critério (o que poderia distorcer as análises)."""
    for col in numeric_cols:
        if col not in df.columns:
            continue
        df = df.withColumn(col, F.col(col).cast(DoubleType()))
        df = df.withColumn(f"{col}_era_nulo", F.col(col).isNull())
    return df


def _meta_long(meta_municipio: DataFrame) -> DataFrame:
    """A tabela de metas vem em formato 'largo': uma coluna por ano
    (meta_alfabetizacao_2024 ... meta_alfabetizacao_2030). Como o
    indicador também é anual, "desempilhamos" essas colunas em formato
    longo (id_municipio, ano, meta_percentual) para poder integrar com
    o indicador via join em (id_municipio, ano).

    Quando há mais de uma linha por município/ano (ex.: uma por rede de
    ensino — municipal, estadual etc.), consolidamos com a média.
    """
    anos = range(2024, 2031)
    partes = []
    for ano in anos:
        col_name = f"meta_alfabetizacao_{ano}"
        if col_name in meta_municipio.columns:
            partes.append(
                meta_municipio.select(
                    "id_municipio",
                    F.lit(ano).alias("ano"),
                    F.col(col_name).cast(DoubleType()).alias("meta_percentual"),
                )
            )
    meta_long = reduce(DataFrame.unionByName, partes)
    return meta_long.groupBy("id_municipio", "ano").agg(
        F.avg("meta_percentual").alias("meta_percentual")
    )


def build_silver_alfabetizacao(spark: SparkSession) -> DataFrame:
    # A tabela 'municipio' já traz sigla_uf/nome_uf prontos, então não é
    # necessário um join separado com a tabela 'uf' para essas colunas.
    municipio = _normalizar_chaves(_limpar(read_bronze(spark, "municipio")))
    indicador = _normalizar_chaves(_limpar(read_bronze(spark, "indicador_alfabetizacao")))
    meta_municipio = _normalizar_chaves(_limpar(read_bronze(spark, "meta_alfabetizacao_municipio")))

    indicador = _tratar_ausentes(
        indicador, numeric_cols=["proficiencia_media", "percentual_alfabetizado"]
    )
    meta_long = _meta_long(meta_municipio)

    df = (
        indicador.alias("i")
        .join(
            municipio.select(
                "id_municipio", "sigla_uf", "nome_uf", F.col("nome").alias("nome_municipio")
            ).alias("m"),
            on="id_municipio",
            how="left",
        )
        .join(meta_long.alias("meta"), on=["id_municipio", "ano"], how="left")
    )

    df = df.select(
        "id_municipio",
        "nome_municipio",
        "sigla_uf",
        "nome_uf",
        "ano",
        "proficiencia_media",
        "percentual_alfabetizado",
        "meta_percentual",
        "proficiencia_media_era_nulo",
        "percentual_alfabetizado_era_nulo",
    ).withColumn("_dt_processamento", F.current_timestamp())

    return df


def write_silver(df: DataFrame) -> str:
    path = f"gs://{settings.bucket_silver}/indicador_alfabetizacao/"
    (
        df.write.mode("overwrite")
        .partitionBy("ano", "sigla_uf")  # particionamento reduz custo de scan (FinOps)
        .parquet(path)
    )
    return path


def main() -> None:
    spark = get_spark()
    with track_job("transform_silver", layer="silver") as ctx:
        df = build_silver_alfabetizacao(spark)
        ctx["rows"] = df.count()
        path = write_silver(df)
        print(f"[silver] {ctx['rows']} linhas -> {path}")
    spark.stop()


if __name__ == "__main__":
    main()
