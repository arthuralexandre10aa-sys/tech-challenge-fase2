"""
Camada Bronze — Ingestão Streaming

Consome eventos quase em tempo real do tópico Pub/Sub
"indicadores-alfabetizacao-eventos" — simulando atualizações de
indicadores, novas medições de desempenho e atualizações de metas —
e grava em micro-lotes (a cada 60s ou 500 mensagens) em Parquet na
área de streaming da camada Bronze.

O produtor de eventos está em streaming/producer_simulator.py.

Execução local (com o emulador do docker-compose):
    python -m src.bronze.ingest_streaming
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import pandas as pd
from google.cloud import pubsub_v1

from loguru import logger

from src.utils.config import settings
from src.utils.observability import track_job

MICROBATCH_MAX_MESSAGES = 500
MICROBATCH_MAX_SECONDS = 60


def _flush(buffer: list[dict]) -> None:
    if not buffer:
        return
    df = pd.DataFrame(buffer)
    ingest_ts = datetime.now(timezone.utc)
    path = (
        f"gs://{settings.bucket_bronze}/streaming_eventos/"
        f"dt_ingestao={ingest_ts.date().isoformat()}/"
        f"batch_{ingest_ts.strftime('%H%M%S')}.parquet"
    )
    df["_dt_ingestao"] = ingest_ts.isoformat()
    df["_fonte"] = "pubsub_streaming"
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
    print(f"[bronze/streaming] micro-lote com {len(df)} eventos -> {path}")


def run(max_runtime_seconds: int | None = None) -> None:
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        settings.gcp_project_id, settings.pubsub_subscription
    )

    buffer: list[dict] = []
    window_start = time.time()
    start_time = time.time()

    with track_job("ingest_streaming", layer="bronze") as ctx:
        while True:
            if max_runtime_seconds and (time.time() - start_time) > max_runtime_seconds:
                break

            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 100},
                timeout=10,
            )

            ack_ids = []
            for received in response.received_messages:
                try:
                    event = json.loads(received.message.data.decode("utf-8"))
                    buffer.append(event)
                    ack_ids.append(received.ack_id)
                except json.JSONDecodeError as exc:
                    logger.warning(f"Evento malformado no tópico de streaming: {exc}")

            if ack_ids:
                subscriber.acknowledge(
                    request={"subscription": subscription_path, "ack_ids": ack_ids}
                )

            elapsed_window = time.time() - window_start
            if len(buffer) >= MICROBATCH_MAX_MESSAGES or elapsed_window >= MICROBATCH_MAX_SECONDS:
                ctx["rows"] += len(buffer)
                _flush(buffer)
                buffer = []
                window_start = time.time()

        # flush final ao encerrar
        ctx["rows"] += len(buffer)
        _flush(buffer)


if __name__ == "__main__":
    run()
