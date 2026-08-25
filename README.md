# Tech Challenge – Fase 2
## Pipeline Híbrido para Análise da Alfabetização no Brasil

> Projeto integrador da Fase 2 — Pós-Tech Data Analytics/Engineering.
> Pipeline de dados híbrido (Batch + Streaming), seguindo a Arquitetura
> Medalhão (Bronze/Silver/Gold), para integração e análise dos
> indicadores do **Compromisso Nacional Criança Alfabetizada**.

---

## Sumário

1. [Contexto do problema](#1-contexto-do-problema)
2. [Objetivo técnico](#2-objetivo-técnico)
3. [Arquitetura da solução](#3-arquitetura-da-solução)
4. [Diagrama e fluxo de dados](#4-diagrama-e-fluxo-de-dados)
5. [Tecnologias utilizadas](#5-tecnologias-utilizadas)
6. [Decisões arquiteturais (trade-offs)](#6-decisões-arquiteturais-trade-offs)
7. [Qualidade de dados](#7-qualidade-de-dados)
8. [FinOps](#8-finops)
9. [Aplicação em IA](#9-aplicação-em-ia)
10. [Estrutura do repositório](#10-estrutura-do-repositório)
11. [Como executar](#11-como-executar)
12. [Fluxo de trabalho Git (branches, commits, PRs)](#12-fluxo-de-trabalho-git)
13. [Roteiro do vídeo executivo](#13-roteiro-do-vídeo-executivo)

---

## 1. Contexto do problema

A alfabetização na infância é um dos pilares do desenvolvimento
educacional, social e econômico do país. O **Compromisso Nacional
Criança Alfabetizada** mobiliza União, estados, Distrito Federal e
municípios para garantir que todas as crianças brasileiras estejam
alfabetizadas até o final do 2º ano do ensino fundamental.

Em 2023, o INEP realizou a *Pesquisa Alfabetiza Brasil*, que definiu o
ponto de corte de **743 pontos** na escala de proficiência do Saeb como
critério de alfabetização. A partir daí, foi criado o **Indicador
Criança Alfabetizada**, que mede o percentual de estudantes que atingem
esse patamar — com meta nacional de alfabetização plena até **2030**.

Entender os fatores que influenciam esse processo exige integrar
diferentes fontes: metas nacionais/estaduais/municipais, dados
territoriais, microdados educacionais e indicadores de desempenho. Este
projeto constrói exatamente essa integração, usando dados públicos da
plataforma **[Base dos Dados](https://basedosdados.org/)**.

## 2. Objetivo técnico

Construir uma pipeline de dados escalável em nuvem que realize:

- Ingestão de diferentes fontes de dados educacionais (batch + streaming);
- Tratamento e padronização das informações;
- Integração entre bases heterogêneas;
- Disponibilização de uma camada analítica confiável (Gold);
- Controle de custos da infraestrutura (FinOps).

## 3. Arquitetura da solução

A solução é implementada em **Google Cloud Platform (GCP)** — a escolha
é justificada na seção 6 — e segue a **Arquitetura Medalhão**:

### Ingestão Batch
Processamento diário (orquestrado pelo Airflow/Cloud Composer) para as
fontes de baixa frequência de mudança: metas educacionais, dados de
município/UF e o indicador oficial de alfabetização, extraídos
diretamente das tabelas públicas da Base dos Dados no BigQuery.

### Ingestão Streaming
Um produtor (`streaming/producer_simulator.py`) simula eventos quase em
tempo real — atualização de indicadores, novas medições de desempenho e
mudanças de metas — publicados em um tópico **Pub/Sub**. Um consumidor
grava micro-lotes (60s ou 500 mensagens) continuamente na camada Bronze.

### Bronze — Raw Data
Dados brutos, sem transformação, armazenados em **Parquet** no GCS,
particionados por data de ingestão, preservando o histórico completo.

### Silver — Dados Tratados
Job **PySpark** que aplica limpeza, tratamento de valores ausentes,
padronização de nomes/tipos, normalização de chaves e **integração**
entre as bases (join município + UF + indicador + metas), seguido das
checagens de qualidade de dados.

### Gold — Camada Analítica
Três datasets prontos para consumo:
- `gold_indicador_por_municipio`
- `gold_meta_vs_resultado`
- `gold_evolucao_temporal_uf`

Publicados em Parquet (GCS) e como tabelas no **BigQuery**, para consumo
por dashboards, análises estatísticas e modelos de machine learning.

## 4. Diagrama e fluxo de dados

O diagrama completo (Mermaid) e a descrição textual do fluxo de dados
estão em **[`architecture/diagram.md`](architecture/diagram.md)**.

Resumo do fluxo:

```
Base dos Dados (BigQuery) ─┐
                            ├─▶ Batch (Airflow) ─┐
Eventos simulados ─▶ Pub/Sub ─▶ Streaming ────────┤
                                                   ▼
                                              Bronze (GCS/Parquet)
                                                   │
                                     limpeza · padronização · joins
                                                   ▼
                                              Silver (GCS/Parquet)
                                                   │
                                          checagens de qualidade
                                                   ▼
                                          Gold (GCS + BigQuery)
                                                   │
                                     Dashboards · Estatística · ML
```

## 5. Tecnologias utilizadas

| Categoria | Ferramenta | Justificativa |
|---|---|---|
| Cloud | **Google Cloud Platform** | A fonte de dados (Base dos Dados) é nativa do BigQuery; usar GCP elimina egress/transferência entre nuvens e permite consultar os dados públicos sem duplicá-los antecipadamente |
| Storage (Data Lake) | **Google Cloud Storage + Parquet** | Armazenamento barato, durável, e formato colunar eficiente para analytics |
| Processamento batch | **PySpark** | Escalável para volumes crescentes; roda local no desafio e em cluster gerenciado (Dataproc/EMR) se o volume crescer |
| Streaming | **Pub/Sub + Cloud Run (scale-to-zero)** | Serverless, paga por uso, ideal para volume de eventos variável/baixo |
| Orquestração | **Apache Airflow (Cloud Composer)** | Padrão de mercado para DAGs, dependências, retries e alertas |
| Camada analítica | **BigQuery** | Consultas SQL rápidas sobre a Gold, integração nativa com BI |
| Qualidade de dados | **Validações customizadas em PySpark** | Regras simples e testáveis de duplicidade, nulos e integridade referencial |
| Versionamento | **Git/GitHub** | Histórico de commits, branches e Pull Requests |

## 6. Decisões arquiteturais (trade-offs)

### Batch vs. Streaming
A fonte oficial do indicador é atualizada em ciclos anuais/bianuais —
não há necessidade real de streaming *para os dados oficiais*. Por isso
adotamos uma arquitetura **híbrida**: batch diário para as fontes
estruturais (metas, município, indicador oficial) e streaming para o
cenário de eventos mais granulares (medições contínuas, simuladas neste
projeto), evitando o custo e a complexidade de rodar tudo em streaming
sem necessidade real.

### Data Lake vs. Data Warehouse
Optamos por um **Data Lake (GCS)** para as camadas Bronze/Silver — onde
flexibilidade de schema e custo de storage barato importam mais — e um
**Data Warehouse (BigQuery)** apenas na camada Gold, onde SQL rápido e
integração com ferramentas de BI são prioridade. Essa combinação (Lakehouse)
evita pagar o custo mais alto de storage/compute do DW para dados brutos
que raramente são consultados diretamente.

### Custo vs. Performance
- Sem cluster fixo: os jobs PySpark rodam com `SparkSession` local,
  disparados pelo próprio Airflow — evita o custo de manter um cluster
  Spark sempre ligado para um volume de dados pequeno/médio.
- Streaming em **Cloud Run scale-to-zero** em vez de uma VM/Kafka
  dedicado: custo quase nulo em baixo volume, com trade-off de latência
  de cold start aceitável para este caso de uso (não é tempo real crítico).
- Particionamento agressivo (por `ano`/`sigla_uf`) reduz bytes escaneados
  no BigQuery (custo) em troca de uma organização de diretórios um pouco
  mais complexa.

Mais detalhes de custo em **[`docs/finops.md`](docs/finops.md)**.

## 7. Qualidade de dados

Implementadas em `src/quality/data_quality_checks.py` e cobertas por
testes unitários em `tests/test_data_quality.py`:

- **Verificação de duplicidade** — chaves `(id_municipio, ano)` devem ser únicas;
- **Detecção de valores ausentes** — percentual de nulos por coluna, com limite configurável;
- **Validação de chaves de relacionamento** — todo `id_municipio` do
  indicador deve existir na dimensão município (detecção de órfãos);
- **Consistência entre tabelas** — percentuais devem estar no intervalo [0, 100].

Falhas críticas (duplicidade, chaves órfãs, valores fora de domínio)
**interrompem o pipeline**; valores ausentes dentro do limite são
tratados (imputação/flag) sem bloquear a execução. Cada checagem é
registrada em log (`src/utils/observability.py`) para rastreabilidade.

## 8. FinOps

Documentado em detalhe em **[`docs/finops.md`](docs/finops.md)**,
cobrindo armazenamento eficiente (Parquet + particionamento + lifecycle),
otimização de queries, controle de recursos computacionais (clusters
efêmeros, serverless) e uma estimativa de custo mensal da arquitetura.

## 9. Aplicação em IA

A camada Gold foi desenhada para alimentar diretamente iniciativas de IA:

- **Modelos de predição de alfabetização**: `gold_indicador_por_municipio`
  pode treinar modelos de regressão/classificação para prever risco de
  município não atingir a meta.
- **Análise de desigualdade educacional**: `gold_evolucao_temporal_uf`
  permite clusterização de municípios/UFs por padrão de evolução do
  indicador, identificando grupos de vulnerabilidade educacional.
- **Políticas públicas baseadas em dados**: `gold_meta_vs_resultado`
  (com o campo `gap_para_meta`) prioriza onde investir recursos,
  permitindo simulações de cenário ("what-if") para 2030.

## 10. Estrutura do repositório

```
tech-challenge-fase2/
├── README.md                       # este documento
├── requirements.txt
├── docker-compose.yml              # ambiente local (Airflow + emulador Pub/Sub)
├── .env.example
├── architecture/
│   └── diagram.md                  # diagrama Mermaid + fluxo de dados
├── docs/
│   └── finops.md                   # detalhamento de custos
├── src/
│   ├── bronze/
│   │   ├── ingest_batch.py         # ingestão batch (Base dos Dados -> Bronze)
│   │   └── ingest_streaming.py     # consumidor Pub/Sub -> Bronze
│   ├── silver/
│   │   └── transform_silver.py     # limpeza, padronização, integração
│   ├── gold/
│   │   └── build_gold.py           # datasets analíticos
│   ├── quality/
│   │   └── data_quality_checks.py  # regras de qualidade de dados
│   └── utils/
│       ├── config.py                # configuração central
│       └── observability.py         # logging dos jobs
├── streaming/
│   └── producer_simulator.py       # simulador de eventos em tempo quase real
├── airflow/
│   └── dags/
│       └── pipeline_alfabetizacao_dag.py
└── tests/
    └── test_data_quality.py
```

## 11. Como executar

### Pré-requisitos
- Python 3.11+
- Docker e Docker Compose (para o ambiente local de Airflow)
- Conta de serviço GCP com acesso ao BigQuery público da Base dos Dados

### Setup

```bash
git clone <url-deste-repositorio>
cd tech-challenge-fase2
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # preencher com suas credenciais/projeto GCP
```

### Rodando a pipeline batch localmente

```bash
python -m src.bronze.ingest_batch --tabela all
python -m src.silver.transform_silver
python -m src.quality.data_quality_checks
python -m src.gold.build_gold
```

> Os scripts de Silver/Gold/Qualidade usam PySpark; rodando localmente,
> uma `SparkSession` local (`master("local[*]")`) é suficiente — não é
> necessário um cluster.

### Simulando o streaming

```bash
docker compose up pubsub-emulator -d
python streaming/producer_simulator.py --eventos-por-segundo 5 &
python -m src.bronze.ingest_streaming
```

### Subindo o Airflow local

```bash
docker compose up airflow
# acessar http://localhost:8080 (usuário/senha exibidos no log do container)
```

### Rodando os testes

```bash
pytest tests/ -v
```

## 12. Fluxo de trabalho Git

Este repositório segue o modelo **GitHub Flow**:

- `main` — sempre estável/deployável;
- branches de funcionalidade: `feature/ingestao-batch`,
  `feature/transformacao-silver`, `feature/camada-gold`,
  `feature/qualidade-dados`, `feature/airflow-dag`;
- cada funcionalidade é integrada via **Pull Request**, com descrição do
  que foi alterado e por quê, revisão de ao menos um integrante do grupo
  e discussão registrada nos comentários da PR antes do merge em `main`;
- mensagens de commit seguem o padrão `tipo(escopo): descrição`, por
  exemplo `feat(bronze): adiciona ingestão batch da Base dos Dados` ou
  `fix(silver): corrige normalização de sigla_uf`.

## 13. Roteiro do vídeo executivo

Sugestão de roteiro para o vídeo de até 5 minutos (linguagem executiva,
como se fosse para stakeholders/liderança):

1. **(0:00–1:00) Problema de negócio** — por que a alfabetização até 2030
   é uma prioridade nacional e por que dados fragmentados dificultam o
   acompanhamento da meta.
2. **(1:00–2:30) Arquitetura da solução** — mostrar o diagrama
   (`architecture/diagram.md`), explicar a jornada Bronze → Silver → Gold
   e a combinação batch + streaming em linguagem simples.
3. **(2:30–3:30) Valor da pipeline** — como a camada Gold entrega,
   de forma confiável e atualizada, indicadores por município,
   comparação com metas e evolução temporal para tomada de decisão.
4. **(3:30–4:30) Potencial de IA** — como os dados Gold viabilizam
   modelos preditivos e identificação de municípios prioritários.
5. **(4:30–5:00) Encerramento** — custo controlado (FinOps) e próximos passos.
