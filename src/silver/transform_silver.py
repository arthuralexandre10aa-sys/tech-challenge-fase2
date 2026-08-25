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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

from src.utils.config import settings
from src.utils.observability import track_job


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver_alfabetizacao")
        .config("spark.sql.shuffle.partitions", "8")  # dataset pequeno/médio: evita overhead
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, table_key: str) -> DataFrame:
    return spark.read.parquet(f"gs://{settings.bucket_bronze}/{table_key}/")


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


def build_silver_alfabetizacao(spark: SparkSession) -> DataFrame:
    municipio = _normalizar_chaves(_limpar(read_bronze(spark, "municipio")))
    uf = _normalizar_chaves(_limpar(read_bronze(spark, "uf")))
    indicador = _normalizar_chaves(_limpar(read_bronze(spark, "indicador_alfabetizacao")))
    meta_municipio = _normalizar_chaves(_limpar(read_bronze(spark, "meta_alfabetizacao_municipio")))

    indicador = _tratar_ausentes(
        indicador, numeric_cols=["proficiencia_media", "percentual_alfabetizado"]
    )

    df = (
        indicador.alias("i")
        .join(municipio.select("id_municipio", "sigla_uf", "nome").alias("m"), on="id_municipio", how="left")
        .join(uf.select("sigla_uf", F.col("nome").alias("nome_uf")).alias("u"), on="sigla_uf", how="left")
        .join(
            meta_municipio.select("id_municipio", "ano", "meta_percentual").alias("meta"),
            on=["id_municipio", "ano"],
            how="left",
        )
    )

    df = df.select(
        "id_municipio",
        F.col("m.nome").alias("nome_municipio"),
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
