# FinOps — Otimização de Custos

Este documento detalha as decisões que reduzem o custo operacional da
arquitetura, conforme exigido no desafio.

## 1. Armazenamento eficiente

| Prática | Como foi aplicada | Impacto no custo |
|---|---|---|
| Formato colunar (Parquet + Snappy) | Todas as camadas (Bronze/Silver/Gold) usam Parquet | Reduz volume em disco em ~70-80% vs. CSV/JSON e acelera leitura (menos I/O cobrado) |
| Particionamento | Bronze por `dt_ingestao`; Silver/Gold por `ano`/`sigla_uf` | Queries analíticas (ex.: "indicador do Ceará em 2025") só escaneiam as partições relevantes, não a tabela inteira |
| Ciclo de vida (lifecycle) do GCS | Bronze com regra de transição para Coldline após 90 dias e exclusão após 2 anos | Dados brutos antigos raramente são reacessados — armazená-los em Standard indefinidamente é desperdício |
| Camada Gold enxuta | Apenas datasets agregados/curados, sem colunas não utilizadas | Menor volume = menor custo de storage e de scan no BigQuery |

## 2. Otimização de queries

- **BigQuery**: tabelas Gold expostas como tabelas nativas particionadas e
  com *clustering* por `sigla_uf`, reduzindo bytes escaneados por consulta
  (o modelo de cobrança do BigQuery on-demand é por bytes lidos).
- **Spark (Silver/Gold)**: número de partições (`spark.sql.shuffle.partitions`)
  ajustado ao volume real dos dados (dataset médio, não "big data"),
  evitando overhead de milhares de tasks pequenas.
- **Predicate pushdown**: leituras sempre filtram por partição antes de
  qualquer transformação, em vez de carregar tudo e filtrar em memória.

## 3. Controle de recursos computacionais

- **Spark local, sem cluster dedicado**: os jobs Silver/Gold rodam com
  `SparkSession` local, disparados dentro do próprio worker do Airflow.
  Para o volume de dados deste desafio, isso elimina o custo de manter
  (ou subir/descer) um cluster Spark gerenciado (Dataproc/EMR).
- **Streaming dimensionado por carga real**: o consumidor Pub/Sub roda em
  Cloud Run (scale-to-zero) em vez de uma VM fixa, escalando com o volume
  de eventos e custando ~zero fora dos períodos de pico.
- **Orquestração (Airflow/Cloud Composer)**: agendamento em horário de
  menor custo de compute (03h) e `max_active_runs=1` para não concorrer
  por recursos.

## 4. Batch vs. Streaming — trade-off de custo

A fonte oficial (Base dos Dados) é atualizada em ciclos anuais/bianuais.
Manter um pipeline de streaming full-time para uma fonte que muda
raramente seria desperdício de custo. Por isso:

- O **batch diário** cobre a necessidade real de atualização das metas e
  indicadores oficiais, a um custo previsível e baixo.
- O **streaming** é reservado para o cenário em que múltiplas secretarias
  municipais ou escolas alimentam eventos com maior frequência (medições
  contínuas de desempenho) — cenário simulado no desafio — e usa
  componentes *serverless* (Pub/Sub + Cloud Run) que só cobram por uso.

## 5. Estimativa de custo (ordem de grandeza, mensal)

| Item | Configuração | Estimativa |
|---|---|---|
| GCS (Bronze+Silver+Gold) | ~50 GB, Standard + Coldline | US$ 2–5 |
| BigQuery (queries Gold) | ~5 GB escaneados/dia, on-demand | US$ 5–10 |
| Cloud Composer (Airflow, executa PySpark local) | ambiente small | US$ 90–120 |
| Pub/Sub + Cloud Run (streaming) | baixo volume simulado | US$ 1–5 |
| **Total estimado** | | **~US$ 100–140/mês** |

> Observação: o item de maior custo é o ambiente gerenciado do Airflow
> (Cloud Composer). Para o escopo do desafio, uma alternativa mais barata
> é rodar o Airflow localmente via `docker-compose.yml` (fornecido neste
> repositório) ou usar Cloud Scheduler + Cloud Functions para orquestração
> simplificada, eliminando esse custo fixo em um cenário de baixo volume.
