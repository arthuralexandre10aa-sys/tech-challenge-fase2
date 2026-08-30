"""
Dashboard simples da Camada Gold

Lê os 3 datasets analíticos da camada Gold (direto do bucket GCS) e
gera um único arquivo HTML autocontido (gráficos embutidos como
imagem, sem servidor, sem JavaScript externo) com KPIs e gráficos.

Execução:
    python -m dashboard.build_dashboard

Gera: dashboard/gold_dashboard.html — basta abrir no navegador.
"""
from __future__ import annotations

import base64
import io
from datetime import datetime

import matplotlib
matplotlib.use("Agg")  # sem interface gráfica — só gera imagens
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

from src.utils.config import settings

# ---------- paleta (mesma da apresentação) ----------
NAVY = "#1B2A4A"
GOLD = "#F2A93B"
SLATE = "#4C7A9E"
MUTED = "#667085"
LINE = "#DCE3ED"
BG = "#F7F9FC"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "axes.edgecolor": LINE,
    "axes.labelcolor": NAVY,
    "text.color": NAVY,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "figure.facecolor": "white",
    "axes.facecolor": "white",
})


# ============================================================
# 1. Carrega os dados reais da camada Gold
# ============================================================
def load_gold_data() -> dict[str, pd.DataFrame]:
    base = f"gs://{settings.bucket_gold}"
    return {
        "indicador": pd.read_parquet(f"{base}/gold_indicador_por_municipio/"),
        "meta": pd.read_parquet(f"{base}/gold_meta_vs_resultado/"),
        "evolucao": pd.read_parquet(f"{base}/gold_evolucao_temporal_uf/"),
    }


# ============================================================
# 2. KPIs
# ============================================================
def compute_kpis(data: dict[str, pd.DataFrame]) -> dict:
    ind = data["indicador"]
    meta = data["meta"]
    ultimo_ano = int(ind["ano"].max())

    ind_ultimo = ind[ind["ano"] == ultimo_ano]
    meta_ultimo = meta[meta["ano"] == ultimo_ano]

    pct_atingiu_meta = (
        meta_ultimo["atingiu_meta"].mean() * 100
        if len(meta_ultimo) and "atingiu_meta" in meta_ultimo
        else float("nan")
    )

    return {
        "total_municipios": ind["id_municipio"].nunique(),
        "anos_cobertos": f"{int(ind['ano'].min())}\u2013{ultimo_ano}",
        "percentual_medio_nacional": ind_ultimo["percentual_alfabetizado"].mean(),
        "proficiencia_media_nacional": ind_ultimo["proficiencia_media"].mean(),
        "pct_municipios_atingiu_meta": pct_atingiu_meta,
        "ultimo_ano": ultimo_ano,
    }


# ============================================================
# 3. Gráficos (cada função devolve uma figura matplotlib)
# ============================================================
def fig_uf_ultimo_ano(evolucao: pd.DataFrame, ultimo_ano: int) -> plt.Figure:
    df = evolucao[evolucao["ano"] == ultimo_ano].sort_values(
        "percentual_alfabetizado_medio", ascending=True
    )
    media_nacional = df["percentual_alfabetizado_medio"].mean()
    colors = [GOLD if v >= media_nacional else SLATE for v in df["percentual_alfabetizado_medio"]]

    fig, ax = plt.subplots(figsize=(7.4, max(3.5, 0.28 * len(df))))
    ax.barh(df["sigla_uf"], df["percentual_alfabetizado_medio"], color=colors, height=0.65)
    ax.axvline(media_nacional, color=NAVY, linestyle="--", linewidth=1, alpha=0.6)
    ax.text(
        media_nacional + 0.5, len(df) - 0.5, f"média {media_nacional:.1f}%",
        color=NAVY, fontsize=9, va="top",
    )
    ax.set_xlabel("% médio de alfabetização")
    ax.set_title(f"Percentual de alfabetização por UF — {ultimo_ano}", fontsize=12, fontweight="bold", loc="left")
    ax.spines[["top", "right"]].set_visible(False)
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=100))
    fig.tight_layout()
    return fig


def fig_evolucao_nacional(evolucao: pd.DataFrame) -> plt.Figure:
    nacional = (
        evolucao.groupby("ano")["percentual_alfabetizado_medio"]
        .mean()
        .reset_index()
        .sort_values("ano")
    )
    fig, ax = plt.subplots(figsize=(6.2, 3.6))
    if len(nacional) == 1:
        ax.bar(nacional["ano"].astype(str), nacional["percentual_alfabetizado_medio"], color=SLATE, width=0.4)
    else:
        ax.plot(
            nacional["ano"], nacional["percentual_alfabetizado_medio"],
            color=SLATE, marker="o", linewidth=2.5, markersize=7,
            markerfacecolor=GOLD, markeredgecolor=SLATE,
        )
        ax.set_xticks(nacional["ano"])
    for x, y in zip(nacional["ano"], nacional["percentual_alfabetizado_medio"]):
        ax.annotate(f"{y:.1f}%", (x, y), textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=9, color=NAVY, fontweight="bold")
    ax.set_ylabel("% médio nacional")
    ax.set_title("Evolução do percentual médio nacional", fontsize=12, fontweight="bold", loc="left")
    ax.spines[["top", "right"]].set_visible(False)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=100))
    fig.tight_layout()
    return fig


def fig_distribuicao(indicador: pd.DataFrame, ultimo_ano: int) -> plt.Figure:
    df = indicador[indicador["ano"] == ultimo_ano]
    fig, ax = plt.subplots(figsize=(6.2, 3.6))
    ax.hist(df["percentual_alfabetizado"].dropna(), bins=24, color=SLATE, edgecolor="white")
    media = df["percentual_alfabetizado"].mean()
    ax.axvline(media, color=GOLD, linewidth=2.2)
    ax.text(media, ax.get_ylim()[1] * 0.95, f" média {media:.1f}%", color=NAVY, fontsize=9, fontweight="bold")
    ax.set_xlabel("% de alfabetização do município")
    ax.set_ylabel("Nº de municípios")
    ax.set_title(f"Distribuição do indicador entre municípios — {ultimo_ano}", fontsize=12, fontweight="bold", loc="left")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    return fig


def fig_gap_meta(meta: pd.DataFrame, ultimo_ano: int, n: int = 10) -> plt.Figure:
    df = meta[meta["ano"] == ultimo_ano].dropna(subset=["gap_para_meta"])
    piores = df.nsmallest(n, "gap_para_meta").sort_values("gap_para_meta", ascending=True)

    fig, ax = plt.subplots(figsize=(7.4, max(3.2, 0.32 * len(piores))))
    labels = piores["nome_municipio"].fillna(piores["id_municipio"].astype(str)) + " (" + piores["sigla_uf"] + ")"
    ax.barh(labels, piores["gap_para_meta"], color=NAVY, height=0.6)
    ax.axvline(0, color=MUTED, linewidth=1)
    ax.set_xlabel("Distância da meta (pontos percentuais)")
    ax.set_title(f"{n} municípios mais distantes da meta — {ultimo_ano}", fontsize=12, fontweight="bold", loc="left")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    return fig


# ============================================================
# 4. Monta o HTML final
# ============================================================
def fig_to_base64(fig: plt.Figure) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=160, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def kpi_card(label: str, value: str, accent: bool = False) -> str:
    bg = GOLD if accent else NAVY
    fg = NAVY if accent else "white"
    sub = "#2A3550" if accent else "#C7D2E6"
    return f"""
    <div class="kpi" style="background:{bg}; color:{fg};">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label" style="color:{sub};">{label}</div>
    </div>"""


def chart_card(title: str, img_b64: str) -> str:
    return f"""
    <div class="card">
        <img src="data:image/png;base64,{img_b64}" alt="{title}" />
    </div>"""


def render_html(kpis: dict, images: dict[str, str], output_path: str) -> None:
    gerado_em = datetime.now().strftime("%d/%m/%Y %H:%M")
    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8" />
<title>Dashboard — Camada Gold | Alfabetização</title>
<style>
    * {{ box-sizing: border-box; }}
    body {{
        margin: 0; padding: 0 0 3rem 0;
        background: {BG};
        font-family: -apple-system, "Segoe UI", Arial, sans-serif;
        color: {NAVY};
    }}
    header {{
        background: {NAVY}; color: white;
        padding: 2.2rem 3rem 1.8rem 3rem;
    }}
    header .kicker {{
        color: {GOLD}; font-weight: 700; letter-spacing: 2px;
        font-size: 0.8rem; text-transform: uppercase;
    }}
    header h1 {{ margin: 0.4rem 0 0.3rem 0; font-size: 1.9rem; }}
    header p {{ margin: 0; color: #AFC0DA; font-size: 0.9rem; }}
    .wrap {{ max-width: 1180px; margin: 0 auto; padding: 0 2rem; }}
    .kpis {{
        display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem;
        margin-top: -2.2rem;
    }}
    .kpi {{
        border-radius: 12px; padding: 1.1rem 1.3rem;
        box-shadow: 0 8px 20px rgba(27,42,74,0.15);
    }}
    .kpi-value {{ font-size: 1.9rem; font-weight: 700; }}
    .kpi-label {{ font-size: 0.8rem; margin-top: 0.2rem; }}
    .grid {{
        display: grid; grid-template-columns: 1fr 1fr; gap: 1.2rem;
        margin-top: 2rem;
    }}
    .grid .full {{ grid-column: 1 / -1; }}
    .card {{
        background: white; border: 1px solid {LINE}; border-radius: 12px;
        padding: 1rem; box-shadow: 0 4px 14px rgba(27,42,74,0.06);
    }}
    .card img {{ width: 100%; height: auto; display: block; }}
    footer {{
        max-width: 1180px; margin: 2.5rem auto 0 auto; padding: 0 2rem;
        color: {MUTED}; font-size: 0.8rem;
    }}
</style>
</head>
<body>
    <header>
        <div class="wrap">
            <div class="kicker">Camada Gold</div>
            <h1>Dashboard — Indicador de Alfabetização</h1>
            <p>Gerado em {gerado_em} a partir dos dados reais do bucket Gold</p>
        </div>
    </header>
    <div class="wrap">
        <div class="kpis">
            {kpi_card("Municípios cobertos", f"{kpis['total_municipios']:,}".replace(",", "."))}
            {kpi_card(f"% médio de alfabetização ({kpis['ultimo_ano']})", f"{kpis['percentual_medio_nacional']:.1f}%", accent=True)}
            {kpi_card("Municípios que atingiram a meta", f"{kpis['pct_municipios_atingiu_meta']:.1f}%")}
            {kpi_card("Período coberto", kpis['anos_cobertos'])}
        </div>

        <div class="grid">
            {chart_card("UF último ano", images["uf_ultimo_ano"])}
            {chart_card("Evolução nacional", images["evolucao_nacional"])}
            {chart_card("Distribuição", images["distribuicao"])}
            <div class="full">
                {chart_card("Gap para meta", images["gap_meta"])}
            </div>
        </div>
    </div>
    <footer>
        Tech Challenge — Fase 2 · Pipeline de Alfabetização · dados lidos direto de
        gs://{settings.bucket_gold}
    </footer>
</body>
</html>"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


# ============================================================
# main
# ============================================================
def main() -> None:
    print("[dashboard] lendo dados da camada Gold...")
    data = load_gold_data()

    print("[dashboard] calculando KPIs...")
    kpis = compute_kpis(data)

    print("[dashboard] gerando gráficos...")
    images = {
        "uf_ultimo_ano": fig_to_base64(fig_uf_ultimo_ano(data["evolucao"], kpis["ultimo_ano"])),
        "evolucao_nacional": fig_to_base64(fig_evolucao_nacional(data["evolucao"])),
        "distribuicao": fig_to_base64(fig_distribuicao(data["indicador"], kpis["ultimo_ano"])),
        "gap_meta": fig_to_base64(fig_gap_meta(data["meta"], kpis["ultimo_ano"])),
    }

    output_path = "dashboard/gold_dashboard.html"
    render_html(kpis, images, output_path)
    print(f"[dashboard] pronto! Abra o arquivo: {output_path}")


if __name__ == "__main__":
    main()
