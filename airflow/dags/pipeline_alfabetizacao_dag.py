"""
DAG Airflow — Pipeline Batch de Alfabetização (Bronze -> Silver -> Gold)

Orquestra a ingestão batch diária, seguida das transformações Silver,
checagens de qualidade e construção da camada Gold. A ingestão
streaming roda de forma independente e contínua (serviço separado,
não orquestrado por este DAG).

Cada task PySpark roda com uma SparkSession local, iniciada dentro do
próprio worker do Airflow — abordagem simples, adequada ao volume de
dados deste desafio. Em um cenário de produção com volume maior, essas
tasks poderiam submeter jobs a um cluster Spark gerenciado.

Agendamento: diário às 03h (baixo custo de compute na madrugada — FinOps).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.bronze.ingest_batch import run as ingest_batch_run
from src.gold.build_gold import main as build_gold
from src.quality.data_quality_checks import main as run_quality_checks
from src.silver.transform_silver import main as transform_silver
from src.utils.config import SOURCE_TABLES

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pipeline_alfabetizacao_batch",
    description="Pipeline Medalhão (Bronze/Silver/Gold) do Indicador Criança Alfabetizada",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tech-challenge", "alfabetizacao", "medalhao"],
) as dag:

    ingest_tasks = [
        PythonOperator(
            task_id=f"bronze_ingest_{table_key}",
            python_callable=ingest_batch_run,
            op_kwargs={"table_key": table_key},
        )
        for table_key in SOURCE_TABLES.keys()
    ]

    silver_task = PythonOperator(task_id="silver_transform", python_callable=transform_silver)
    quality_task = PythonOperator(task_id="quality_checks", python_callable=run_quality_checks)
    gold_task = PythonOperator(task_id="gold_build", python_callable=build_gold)

    ingest_tasks >> silver_task >> quality_task >> gold_task
