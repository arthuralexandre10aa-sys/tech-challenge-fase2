"""
Configuração central do pipeline.

Todas as variáveis de ambiente usadas pelos módulos bronze/silver/gold
são carregadas aqui, a partir de um arquivo .env (ver .env.example).
Centralizar a config evita duplicação e facilita trocar de projeto/cloud
em um único lugar (boa prática de FinOps/governança).
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "tech-challenge-alfabetizacao")
    gcp_region: str = os.getenv("GCP_REGION", "southamerica-east1")

    bucket_bronze: str = os.getenv("GCS_BUCKET_RAW", "alfabetizacao-bronze")
    bucket_silver: str = os.getenv("GCS_BUCKET_SILVER", "alfabetizacao-silver")
    bucket_gold: str = os.getenv("GCS_BUCKET_GOLD", "alfabetizacao-gold")

    bq_dataset: str = os.getenv("BIGQUERY_DATASET", "alfabetizacao_gold")
    bd_billing_project: str = os.getenv("BD_BILLING_PROJECT_ID", "tech-challenge-alfabetizacao")

    pubsub_topic: str = os.getenv("PUBSUB_TOPIC", "indicadores-alfabetizacao-eventos")
    pubsub_subscription: str = os.getenv("PUBSUB_SUBSCRIPTION", "indicadores-alfabetizacao-sub")


settings = Settings()

# Tabelas de origem no dataset público "Base dos Dados"
# (br_inep_saeb / indicador criança alfabetizada e correlatos)
SOURCE_TABLES = {
    "uf": "basedosdados.br_bd_diretorios_brasil.uf",
    "municipio": "basedosdados.br_bd_diretorios_brasil.municipio",
    "indicador_alfabetizacao": "basedosdados.br_mec_inep_saeb.indicador_crianca_alfabetizada",
    "meta_alfabetizacao_brasil": "basedosdados.br_mec_pnld.meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf": "basedosdados.br_mec_pnld.meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio": "basedosdados.br_mec_pnld.meta_alfabetizacao_municipio",
}

# Colunas-chave usadas na normalização/join entre bases (camada Silver)
KEY_COLUMNS = {
    "uf": ["sigla_uf"],
    "municipio": ["id_municipio", "sigla_uf"],
    "indicador_alfabetizacao": ["id_municipio", "ano"],
    "meta_alfabetizacao_municipio": ["id_municipio", "ano"],
}
