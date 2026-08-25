"""
Simulador de eventos em tempo quase real.

Como o indicador oficial (Base dos Dados) é atualizado em ciclos anuais/
bianuais, este produtor SIMULA o cenário de streaming descrito no
desafio: atualizações de indicadores, novas medições de desempenho e
mudanças de metas chegando de forma incremental — útil para demonstrar
a arquitetura híbrida (batch + streaming) mesmo com uma fonte de
natureza majoritariamente batch.

Publica eventos JSON no tópico Pub/Sub configurado em .env.

Execução:
    python streaming/producer_simulator.py --eventos-por-segundo 5
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1

from src.utils.config import settings

MUNICIPIOS_AMOSTRA = [
    {"id_municipio": "2304400", "sigla_uf": "CE", "nome": "Fortaleza"},
    {"id_municipio": "2611606", "sigla_uf": "PE", "nome": "Recife"},
    {"id_municipio": "3550308", "sigla_uf": "SP", "nome": "São Paulo"},
    {"id_municipio": "3304557", "sigla_uf": "RJ", "nome": "Rio de Janeiro"},
    {"id_municipio": "5300108", "sigla_uf": "DF", "nome": "Brasília"},
    {"id_municipio": "2307304", "sigla_uf": "CE", "nome": "Juazeiro do Norte"},
]

EVENT_TYPES = [
    "atualizacao_indicador",
    "nova_medicao_desempenho",
    "atualizacao_meta",
]


def build_event() -> dict:
    municipio = random.choice(MUNICIPIOS_AMOSTRA)
    tipo = random.choice(EVENT_TYPES)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": tipo,
        "id_municipio": municipio["id_municipio"],
        "sigla_uf": municipio["sigla_uf"],
        "municipio_nome": municipio["nome"],
        "ano": 2026,
        "proficiencia_media": round(random.uniform(650, 820), 1),
        "percentual_alfabetizado": round(random.uniform(35, 95), 1),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(events_per_second: float, duration_seconds: int | None) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(settings.gcp_project_id, settings.pubsub_topic)

    start = time.time()
    sent = 0
    interval = 1.0 / events_per_second if events_per_second > 0 else 1.0

    while duration_seconds is None or (time.time() - start) < duration_seconds:
        event = build_event()
        publisher.publish(topic_path, json.dumps(event).encode("utf-8"))
        sent += 1
        if sent % 50 == 0:
            print(f"[producer] {sent} eventos publicados")
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulador de eventos de streaming")
    parser.add_argument("--eventos-por-segundo", type=float, default=2.0)
    parser.add_argument("--duracao-segundos", type=int, default=None)
    args = parser.parse_args()
    run(args.eventos_por_segundo, args.duracao_segundos)


if __name__ == "__main__":
    main()
