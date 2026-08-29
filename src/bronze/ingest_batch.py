"""
Camada Bronze — Ingestão Batch

Extrai as tabelas de origem da plataforma "Base dos Dados" (BigQuery
público) e grava os dados brutos, sem transformação, em Parquet no
bucket de Bronze — particionados por data de ingestão para preservar
o histórico completo (auditoria/reprocessamento).

Fontes batch (baixa frequência de mudança):
    - uf
    - municipio
    - indicador_alfabetizacao (Indicador Criança Alfabetizada)
    - meta_alfabetizacao_brasil / _uf / _municipio

Execução:
    python -m src.bronze.ingest_batch --tabela indicador_alfabetizacao
    python -m src.bronze.ingest_batch --tabela all
"""
from __future__ import annotations

import argparse
from datetime import date

import basedosdados as bd
import pandas as pd

from src.utils.config import SOURCE_TABLES, settings
from src.utils.observability import track_job

INGESTION_DATE = date.today().isoformat()


def extract_table(table_key: str) -> pd.DataFrame:
    """Executa a query na Base dos Dados via BigQuery e retorna um DataFrame.

    A tabela 'alunos' (Indicador Criança Alfabetizada) é microdado por
    aluno — agregamos direto na query em (id_municipio, ano) para chegar
    no indicador (% de alfabetizados, ponderado por peso_aluno, e a
    proficiência média), em vez de baixar milhões de linhas de aluno.
    """
    full_table = SOURCE_TABLES[table_key]

    if table_key == "indicador_alfabetizacao":
        query = f"""
            SELECT
                id_municipio,
                ano,
                AVG(proficiencia) AS proficiencia_media,
                ROUND(
                    100 * SUM(CASE WHEN alfabetizado = '1' THEN peso_aluno ELSE 0 END)
                        / SUM(peso_aluno),
                    4
                ) AS percentual_alfabetizado
            FROM `{full_table}`
            GROUP BY id_municipio, ano
        """
    else:
        query = f"SELECT * FROM `{full_table}`"

    df = bd.read_sql(query, billing_project_id=settings.bd_billing_project)
    return df


def write_bronze(df: pd.DataFrame, table_key: str) -> str:
    """Grava o DataFrame como Parquet particionado por data de ingestão.

    Particionar por dt_ingestao (em vez de sobrescrever) é o que garante
    o "histórico completo preservado" exigido na camada Bronze e também
    reduz custo de scan em reprocessamentos pontuais (FinOps).
    """
    path = (
        f"gs://{settings.bucket_bronze}/{table_key}/dt_ingestao={INGESTION_DATE}/"
        f"{table_key}.parquet"
    )
    df["_dt_ingestao"] = INGESTION_DATE
    df["_fonte"] = "base_dos_dados_batch"
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
    return path


def run(table_key: str) -> None:
    with track_job(f"ingest_batch_{table_key}", layer="bronze") as ctx:
        df = extract_table(table_key)
        ctx["rows"] = len(df)
        if df.empty:
            raise ValueError(f"Tabela '{table_key}' retornou vazia — abortando gravação.")
        path = write_bronze(df, table_key)
        print(f"[bronze] {table_key}: {len(df)} linhas -> {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestão batch para a camada Bronze")
    parser.add_argument(
        "--tabela",
        choices=list(SOURCE_TABLES.keys()) + ["all"],
        default="all",
        help="Tabela a ingerir (ou 'all' para todas as fontes batch)",
    )
    args = parser.parse_args()

    tabelas = list(SOURCE_TABLES.keys()) if args.tabela == "all" else [args.tabela]
    for tabela in tabelas:
        run(tabela)


if __name__ == "__main__":
    main()
