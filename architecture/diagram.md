# Diagrama da Pipeline

```mermaid
flowchart TB
    subgraph Fontes["Fontes de Dados — Base dos Dados (BigQuery público)"]
        F1[UF]
        F2[Município]
        F3[Indicador Criança Alfabetizada]
        F4[Metas Brasil / UF / Município]
        F5["Eventos simulados<br/>(medições em tempo quase real)"]
    end

    subgraph Ingestao["Ingestão"]
        B1["Batch diário<br/>(Airflow / Cloud Composer)"]
        S1["Streaming<br/>(Pub/Sub + Cloud Run)"]
    end

    subgraph Bronze["Camada Bronze — Raw"]
        BR1[("GCS: dados brutos<br/>particionados por dt_ingestao")]
    end

    subgraph Silver["Camada Silver — Tratada"]
        SI1["Limpeza, padronização,<br/>normalização de chaves"]
        SI2["Integração das bases<br/>(joins município/UF/metas)"]
        SI3["Checagens de Qualidade<br/>(duplicidade, nulos, FKs)"]
        SI4[("GCS: Parquet particionado<br/>por ano/UF")]
    end

    subgraph Gold["Camada Gold — Analítica"]
        G1[("Indicador por Município")]
        G2[("Meta vs. Resultado")]
        G3[("Evolução Temporal por UF")]
        BQ[("BigQuery — tabelas<br/>para consumo analítico")]
    end

    subgraph Consumo["Consumo"]
        D1["Dashboards<br/>(Looker Studio / Power BI)"]
        D2["Análises Estatísticas"]
        D3["Modelos de ML<br/>(predição de alfabetização)"]
    end

    F1 & F2 & F3 & F4 --> B1
    F5 --> S1
    B1 --> BR1
    S1 --> BR1
    BR1 --> SI1 --> SI2 --> SI3 --> SI4
    SI4 --> G1 & G2 & G3
    G1 & G2 & G3 --> BQ
    BQ --> D1 & D2 & D3
```

## Fluxo de dados (resumo textual)

1. **Extração (Batch)** — Diariamente, o Airflow dispara jobs que leem as
   tabelas `uf`, `município`, `indicador_alfabetizacao` e `meta_*`
   diretamente do BigQuery público da Base dos Dados.
2. **Extração (Streaming)** — Um simulador publica eventos de
   atualização de indicador/meta em um tópico Pub/Sub; um consumidor
   grava micro-lotes (60s ou 500 msgs) continuamente.
3. **Bronze** — Ambos os fluxos pousam sem transformação em GCS,
   particionados por data de ingestão, preservando histórico completo.
4. **Silver** — Um job PySpark (SparkSession local, disparado pelo
   Airflow) lê a Bronze, limpa, padroniza tipos/nomes,
   normaliza chaves, integra as bases via join e passa pelas checagens
   de qualidade antes de gravar.
5. **Gold** — Três datasets analíticos são gerados a partir da Silver e
   publicados também como tabelas no BigQuery.
6. **Consumo** — Dashboards, análises estatísticas e modelos de ML
   consultam a camada Gold via BigQuery.
