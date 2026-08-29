"""
Camada Gold — Datasets Analíticos

A partir da base Silver integrada, cria três datasets analíticos
prontos para consumo por dashboards, análises estatísticas e modelos
de machine learning:

    1. gold_indicador_por_municipio  — indicador por município/ano
    2. gold_meta_vs_resultado        — comparação entre metas e resultados
    3. gold_evolucao_temporal_uf     — evolução do indicador ao longo do tempo (por UF)

Execução:
    spark-submit -m src.gold.build_gold
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config import settings
from src.utils.observability import track_job


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("gold_alfabetizacao")
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.21")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", settings.gcp_service_account_key)
        .getOrCreate()
    )


def read_silver(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(f"gs://{settings.bucket_silver}/indicador_alfabetizacao/")


def indicador_por_municipio(df: DataFrame) -> DataFrame:
    return df.select(
        "id_municipio", "nome_municipio", "sigla_uf", "nome_uf",
        "ano", "proficiencia_media", "percentual_alfabetizado",
    ).orderBy("ano", "sigla_uf", "nome_municipio")


def meta_vs_resultado(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("gap_para_meta", F.col("percentual_alfabetizado") - F.col("meta_percentual"))
        .withColumn("atingiu_meta", F.when(F.col("gap_para_meta") >= 0, F.lit(True)).otherwise(F.lit(False)))
        .select(
            "id_municipio", "nome_municipio", "sigla_uf", "ano",
            "percentual_alfabetizado", "meta_percentual", "gap_para_meta", "atingiu_meta",
        )
    )


def evolucao_temporal_uf(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("sigla_uf", "nome_uf", "ano")
        .agg(
            F.round(F.avg("percentual_alfabetizado"), 2).alias("percentual_alfabetizado_medio"),
            F.round(F.avg("proficiencia_media"), 2).alias("proficiencia_media_uf"),
            F.count("id_municipio").alias("qtd_municipios"),
        )
        .orderBy("sigla_uf", "ano")
    )


def write_gold(df: DataFrame, name: str) -> str:
    path = f"gs://{settings.bucket_gold}/{name}/"
    df.write.mode("overwrite").parquet(path)
    return path


def main() -> None:
    spark = get_spark()
    silver = read_silver(spark)

    datasets = {
        "gold_indicador_por_municipio": indicador_por_municipio(silver),
        "gold_meta_vs_resultado": meta_vs_resultado(silver),
        "gold_evolucao_temporal_uf": evolucao_temporal_uf(silver),
    }

    for name, df in datasets.items():
        with track_job(f"build_{name}", layer="gold") as ctx:
            ctx["rows"] = df.count()
            path = write_gold(df, name)
            print(f"[gold] {name}: {ctx['rows']} linhas -> {path}")

    spark.stop()


if __name__ == "__main__":
    main()
