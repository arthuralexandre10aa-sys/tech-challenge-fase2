"""
Logging básico do pipeline.

Cada job (bronze/silver/gold/quality) registra início, volume
processado e duração em log estruturado — usado para depuração e para
que as checagens de qualidade (obrigatórias) possam registrar e
interromper o pipeline em caso de falha crítica.

(O monitoramento operacional formal — métricas em Cloud Monitoring,
alertas via Slack/e-mail — é um item opcional do desafio e não foi
implementado neste repositório.)
"""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from loguru import logger

logger.add(
    "logs/pipeline_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    enqueue=True,
)


@contextmanager
def track_job(job_name: str, layer: str):
    """Context manager simples que loga início, volume e duração de um job.

    Uso:
        with track_job("ingest_uf", layer="bronze") as ctx:
            df = extract(...)
            ctx["rows"] = len(df)
    """
    start = time.perf_counter()
    ctx: dict = {"rows": 0}
    logger.info(f"[{layer}] iniciando job '{job_name}'")
    try:
        yield ctx
    except Exception as exc:
        logger.error(f"[{layer}] falha no job '{job_name}': {exc}")
        raise
    else:
        elapsed = time.perf_counter() - start
        metric = {
            "job": job_name,
            "layer": layer,
            "rows_processed": ctx.get("rows", 0),
            "duration_seconds": round(elapsed, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        logger.info(f"[{layer}] job '{job_name}' concluído: {json.dumps(metric)}")
