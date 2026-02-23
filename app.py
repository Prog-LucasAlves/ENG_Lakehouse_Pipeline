import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings
from databricks_scripts.conect_databricks import connect_to_databricks
from databricks_scripts.queries import LakehouseQueries

warnings.filterwarnings("ignore")

# ============================================
# CONFIGURAÇÕES DE ESTILO
# ============================================
st.markdown(
    """
<style>
    /* Cabeçalho principal */
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E3A8A;
        padding: 1rem 0;
        border-bottom: 5px solid #2563EB;
        margin-bottom: 2rem;
    }
    .main-header-2 {
        font-size: 2.5rem;
        font-weight: 700;
        color: #ea1a05;
        padding: 1rem 0;
        border-bottom: 2.5px solid #fafafa;
        margin-bottom: 2rem;
    }
    /* Cards de KPI */
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.15);
        text-align: center;
        transition: transform 0.3s;
        margin-top: 20px;
    }
    .kpi-card:hover {
        transform: translateY(-5px);
    }
    .kpi-label {
        font-size: 1rem;
        opacity: 0.9;
        margin-bottom: 0.5rem;
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    .kpi-trend {
        font-size: 0.9rem;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 0.5rem;
    }
    .positive-trend {
        color: #fafafa;
        background: rgba(14, 17, 23, 0.2);
        padding: 0.2rem 0.8rem;
        border-radius: 20px;
    }
    .negative-trend {
        color: #fafafa;
        background: rgba(14, 17, 23, 0.2);
        padding: 0.2rem 0.8rem;
        border-radius: 20px;

    }

    /* Seções */
    .section-title {
        font-size: 1.5rem;
        font-weight: 600;
        color: #1F2937;
        margin: 0.5rem 0 0.25rem 0;
        padding-left: 0.5rem;
        border-left: 5px solid #2563EB;
    }

    /* Cards de métricas secundárias */
     .metric-card {
        background: #9988d2;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.08);
        border: 1px solid #E5E7EB;
        margin-bottom: 20px;  /* Espaço abaixo do card */
        text-align: left;
    }

    /* Cards RFV */
    .rfv-card {
        background: white;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.08);
        border-left: 5px solid;
        margin-bottom: 1rem;
    }

    .segmento-campeoes { border-left-color: #FFD700; }
    .segmento-vip { border-left-color: #C0C0C0; }
    .segmento-fieis { border-left-color: #CD7F32; }
    .segmento-risco { border-left-color: #FF4444; }
    .segmento-hibernando { border-left-color: #FFA500; }
    .segmento-novo { border-left-color: #4CAF50; }

    .segment-tag {
        padding: 0.3rem 1rem;
        border-radius: 20px;
        font-weight: 600;
        display: inline-block;
        margin-bottom: 1rem;
        font-size: 1.1rem;
    }

    .recomendacao-box {
        background: #E3F2FD;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #0A2472;
        margin-top: 1rem;
        font-size: 0.95rem;
    }

    .metric-box {
        background: #F8F9FA;
        padding: 0.8rem;
        border-radius: 10px;
        text-align: center;
        border: 1px solid #E9ECEF;
    }

    /* Cards de entrega */
    .delivery-card {
        background: linear-gradient(135deg, #0A2472 0%, #1E3A8A 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 15px;
        text-align: center;
        box-shadow: 0 10px 30px rgba(0,0,0,0.15);
    }

    .delivery-value {
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0.5rem 0;
    }

    .delivery-label {
        font-size: 1rem;
        opacity: 0.9;
    }

    /* Footer */
    .footer {
        text-align: center;
        color: #6B7280;
        padding: 2rem 0;
        font-size: 0.9rem;
        border-top: 1px solid #E5E7EB;
        margin-top: 3rem;
    }
    .card-item {
        background: #9988d2;
        padding: 20px;
        border-radius: 15px;
        border: 1px solid #E5E7EB;
    }
</style>
""",
    unsafe_allow_html=True,
)

# ============================================
# CONEXÃO COM DATABRICKS
# ============================================
conn = connect_to_databricks()


# ============================================
# FUNÇÕES DE FORMATAÇÃO
# ============================================
def format_currency(value):
    """Formata valor para moeda brasileira"""
    if pd.isna(value) or value is None:
        return "R$ 0,00"
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_percentage(value, decimals=1):
    """Formata valor para porcentagem"""
    if pd.isna(value) or value is None:
        return "0,0%"
    return f"{value:+.{decimals}f}%".replace(".", ",")


def format_number(value):
    """Formata número grande"""
    if pd.isna(value) or value is None:
        return "0"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:.0f}"


def get_segment_color(segmento):
    """Retorna cor do segmento"""
    cores = {
        "🏆 Campeões": "#715cba",
        "💎 Clientes VIP": "#8572C6",
        "⭐ Clientes Fieis": "#9988D2",
        "📈 Potencial Crescimento": "#AD9EDE",
        "⚠️ Em Risco": "#C1B4EA",
        "😴 Hibernando": "#7B66BF",
        "🆕 Novo Cliente": "#6C57B5",
        "🔁 Uma Compra": "#5D48A6",
        "🌱 Cliente Inicial": "#493492",
        "📊 Cliente Regular": "#35207E",
    }
    return cores.get(segmento, "#FFFFFF")


def get_score_class(score):
    """Retorna classe CSS baseada no score"""
    if score >= 4:
        return "score-alto"
    elif score >= 3:
        return "score-medio"
    else:
        return "score-baixo"


# ============================================
# FUNÇÕES DE CARREGAMENTO DE DADOS
# ============================================
@st.cache_data(ttl=300)
def load_kpis(data_inicio, data_fim):
    """Carrega KPIs principais"""
    query = LakehouseQueries.get_kpis_executive(data_inicio, data_fim)
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_vendas_diarias(data_inicio, data_fim):
    """Carrega vendas diárias"""
    query = LakehouseQueries.get_vendas_por_dia(data_inicio, data_fim)
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_top_categorias(data_inicio, data_fim, limite=10):
    """Carrega top categorias"""
    query = LakehouseQueries.get_top_categorias(data_inicio, data_fim, limite)
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_vendas_regiao(data_inicio, data_fim):
    """Carrega vendas por região"""
    query = LakehouseQueries.get_vendas_por_regiao(data_inicio, data_fim)
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_top_produtos(data_inicio, data_fim, limite=20):
    """Carrega top produtos"""
    query = LakehouseQueries.get_top_produtos(data_inicio, data_fim, limite)
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_analise_canais(data_inicio, data_fim):
    """Carrega análise por canais"""
    query = LakehouseQueries.get_analise_canais_venda(data_inicio, data_fim)
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_tempo_entrega(data_inicio, data_fim):
    """Carrega tempo médio de entrega"""
    query = LakehouseQueries.get_tempo_medio_entrega(data_inicio, data_fim)
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_rfv_analysis(data_inicio, data_fim, segmento="Todos"):
    """Carrega análise RFV"""
    query = LakehouseQueries.get_rfv_analysis(
        data_inicio,
        data_fim,
        segmento if segmento != "Todos" else None,
    )
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_status(data_inicio, data_fim):
    """Carrega status dos pedidos"""
    query = LakehouseQueries.get_status(data_inicio, data_fim)
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_region(data_inicio, data_fim):
    """Carrega dados por região"""
    query = LakehouseQueries.get_region(data_inicio, data_fim)
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def load_timeline(data_inicio, data_fim):
    """Carrega timeline de pedidos"""
    query = LakehouseQueries.get_timeline(data_inicio, data_fim)
    df = pd.read_sql(query, conn)
    return df


# ============================================
# FUNÇÕES DE VISUALIZAÇÃO
# ============================================
def create_segment_distribution_chart(df):
    """Gráfico de distribuição por segmento"""
    if df.empty:
        return go.Figure()

    # Agrupar por segmento
    segmentos = (
        df.groupby("segmento_cliente")
        .agg(
            {"id_cliente": "count", "valor_total_gasto": "sum", "ticket_medio": "mean"},
        )
        .reset_index()
    )
    segmentos.columns = ["segmento", "clientes", "receita", "ticket_medio"]
    segmentos = segmentos.sort_values("clientes", ascending=False)

    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("Distribuição de Clientes", "Contribuição em Receita"),
        specs=[[{"type": "pie"}, {"type": "bar"}]],
    )

    # Gráfico de pizza - clientes
    fig.add_trace(
        go.Pie(
            labels=segmentos["segmento"],
            values=segmentos["clientes"],
            textinfo="percent+label",
            hole=0.4,
            marker=dict(colors=[get_segment_color(s) for s in segmentos["segmento"]]),
            textfont=dict(color="white", size=12, family="Arial", weight="bold"),
            insidetextfont=dict(color="white", size=12, family="Arial", weight="bold"),
            outsidetextfont=dict(color="white", size=12, family="Arial", weight="bold"),
        ),
        row=1,
        col=1,
    )

    # Gráfico de barras - receita
    fig.add_trace(
        go.Bar(
            x=segmentos["segmento"],
            y=segmentos["receita"],
            text=segmentos["receita"].apply(lambda x: format_currency(x)),
            textposition="outside",
            marker_color=[get_segment_color(s) for s in segmentos["segmento"]],
        ),
        row=1,
        col=2,
    )

    fig.update_layout(
        height=550,
        showlegend=False,
        title_text="Análise de Segmentos RFV",
        title_x=0.5,
    )

    fig.update_xaxes(tickangle=45, row=1, col=2)

    return fig


def create_rfv_matrix_chart(df):
    """Matriz RFV 3D"""
    if df.empty:
        return go.Figure()

    # Sample para não sobrecarregar
    df_sample = df.sample(min(500, len(df)))

    fig = go.Figure(
        data=[
            go.Scatter3d(
                x=df_sample["score_recencia"],
                y=df_sample["score_frequencia"],
                z=df_sample["score_valor"],
                mode="markers",
                marker=dict(
                    size=df_sample["valor_total_gasto"] / 1000,
                    color=df_sample["rfv_score_total"],
                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(title="Score Total"),
                ),
                text=df_sample.apply(
                    lambda row: f"<b>{row['nome_completo']}</b><br>"
                    f"Segmento: {row['segmento_cliente']}<br>"
                    f"Gasto Total: {format_currency(row['valor_total_gasto'])}<br>"
                    f"Compras: {row['frequencia_compras']}<br>"
                    f"Última Compra: {row['dias_ultima_compra']} dias",
                    axis=1,
                ),
                hoverinfo="text",
            ),
        ],
    )

    fig.update_layout(
        title="Matriz RFV 3D - Clientes",
        scene=dict(
            xaxis_title="Recência (1-5)",
            yaxis_title="Frequência (1-5)",
            zaxis_title="Valor (1-5)",
        ),
        height=600,
    )

    return fig


def create_trend_chart(df, title="Evolução da Receita"):
    """Cria gráfico de tendência com métricas"""
    if df.empty:
        return go.Figure()

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Adicionar receita
    fig.add_trace(
        go.Scatter(
            x=df["data"],
            y=df["receita_total"],
            name="Receita",
            line=dict(color="#715cba", width=3),
            mode="lines+markers",
            marker=dict(size=6),
        ),
        secondary_y=False,
    )

    # Adicionar número de pedidos
    fig.add_trace(
        go.Bar(
            x=df["data"],
            y=df["total_pedidos"],
            name="Pedidos",
            marker_color="#3A7CA5",
            opacity=0.5,
        ),
        secondary_y=True,
    )

    # Adicionar média móvel
    df["media_movel"] = df["receita_total"].rolling(window=7).mean()
    fig.add_trace(
        go.Scatter(
            x=df["data"],
            y=df["media_movel"],
            name="Média Móvel (7d)",
            line=dict(color="#EF4444", width=2, dash="dash"),
        ),
        secondary_y=False,
    )

    fig.update_layout(
        title=dict(text=title, x=0.5),
        hovermode="x unified",
        template="plotly_white",
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    )

    fig.update_xaxes(title_text="Data")
    fig.update_yaxes(title_text="Receita (R$)", secondary_y=False)
    fig.update_yaxes(title_text="Número de Pedidos", secondary_y=True)

    return fig


def create_category_chart(df):
    """Cria gráfico de categorias"""
    if df.empty:
        return go.Figure()

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df["categoria"],
            y=df["receita_total"],
            name="Receita",
            marker_color="#715cba",
            text=df["receita_total"].apply(lambda x: format_currency(x)),
            textposition="outside",
        ),
    )

    fig.update_layout(
        title="Receita por Categoria",
        xaxis_title="Categoria",
        yaxis_title="Receita (R$)",
        template="plotly_white",
        height=500,
        showlegend=False,
    )

    return fig


def create_pie_chart(df, values_col, names_col, title):
    """Cria gráfico de pizza"""
    if df.empty:
        return go.Figure()

    df["formatted_value"] = df[values_col].apply(
        lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
    )

    fig = px.pie(
        df,
        values=values_col,
        names=names_col,
        title=title,
        color_discrete_sequence=["#715cba", "#8572c6", "#ad9ede", "#c1b4ea", "#9988d2"],
        hole=0.4,
    )

    fig.update_traces(
        textposition="inside",
        textinfo="percent+label",
        customdata=df["formatted_value"],
        hovertemplate="<b>%{label}</b><br>Valor: R$ %{customdata}<br>Percentual: %{percent}<extra></extra>",
        textfont=dict(
            color="white",  # Texto branco
            size=12,  # Tamanho do texto
            family="Arial",  # Fonte mais encorpada para melhor visibilidade
        ),
    )

    fig.update_layout(
        height=500,
    )

    return fig


# ============================================
# PÁGINA 1: DASHBOARD DE VENDAS
# ============================================
def pagina_vendas():
    st.markdown(
        '<h1 class="main-header-2">📊 Dashboard de Vendas</h1>',
        unsafe_allow_html=True,
    )

    # Sidebar específica para vendas
    with st.sidebar:
        st.markdown("## 🛒 Filtros de Vendas")

        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input(
                "📅 Data Inicial",
                value=datetime.now() - timedelta(days=90),
                max_value=datetime.now(),
                key="vendas_inicio",
            )
        with col2:
            data_fim = st.date_input(
                "📅 Data Final",
                value=datetime.now(),
                max_value=datetime.now(),
                min_value=data_inicio,
                key="vendas_fim",
            )

        st.markdown("### 📊 Visualizações")
        mostrar_tendencia = st.checkbox(
            "Gráfico de Tendência",
            True,
            key="vendas_tendencia",
        )
        mostrar_categorias = st.checkbox(
            "Análise por Categoria",
            True,
            key="vendas_categorias",
        )
        mostrar_regioes = st.checkbox("Análise por Região", True, key="vendas_regioes")
        mostrar_produtos = st.checkbox("Top Produtos", True, key="vendas_produtos")
        mostrar_canais = st.checkbox("Canais de Venda", True, key="vendas_canais")

        st.markdown("---")
        st.markdown(f"**Atualização:** {datetime.now().strftime('%H:%M:%S')}")

    # Converter datas
    data_inicio_str = data_inicio.strftime("%Y-%m-%d")
    data_fim_str = data_fim.strftime("%Y-%m-%d")

    # Carregar dados
    with st.spinner("Carregando dados de vendas..."):
        df_kpis = load_kpis(data_inicio_str, data_fim_str)
        df_vendas = load_vendas_diarias(data_inicio_str, data_fim_str)
        df_categorias = load_top_categorias(data_inicio_str, data_fim_str)
        df_regioes = load_vendas_regiao(data_inicio_str, data_fim_str)
        df_produtos = load_top_produtos(data_inicio_str, data_fim_str)
        df_canais = load_analise_canais(data_inicio_str, data_fim_str)

    # KPIs
    if not df_kpis.empty:
        kpi = df_kpis.iloc[0]
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-label">💰 RECEITA TOTAL</div>
                    <div class="kpi-value">{format_currency(kpi.get("receita_30d", 0))}</div>
                    <div class="kpi-trend {"trend-positive" if kpi.get("variacao_receita", 0) > 0 else "trend-negative"}">
                        {format_percentage(kpi.get("variacao_receita", 0))} vs período anterior
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with col2:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-label">📦 TOTAL PEDIDOS</div>
                    <div class="kpi-value">{format_number(kpi.get("total_pedidos", 0))}</div>
                    <div class="kpi-trend positive-trend">
                        {format_number(kpi.get("clientes_ativos_30d", 0))} clientes ativos
                    </div>
                </div>
            """,
                unsafe_allow_html=True,
            )

        with col3:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-label">👥 TICKET MÉDIO</div>
                    <div class="kpi-value">{format_currency(kpi.get("ticket_medio", 0))}</div>
                    <div class="kpi-trend {"positive-trend" if kpi.get("variacao_ticket", 0) > 0 else "negative-trend"}">
                        {format_percentage(kpi.get("variacao_ticket", 0))} vs anterior
                    </div>
                </div>
            """,
                unsafe_allow_html=True,
            )

        with col4:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-label">✅ TAXA ENTREGA</div>
                    <div class="kpi-value">{kpi.get("pedidos_entregues", 0) / kpi.get("total_pedidos", 1) * 100:.2f}%</div>
                    <div class="kpi-trend positive-trend">
                        {format_number(kpi.get("pedidos_entregues", 0))} entregues
                    </div>
                </div>
            """,
                unsafe_allow_html=True,
            )

    # Gráfico de tendência
    if mostrar_tendencia and not df_vendas.empty:
        st.markdown(
            '<h2 class="section-title">📈 Evolução Diária de Vendas</h2>',
            unsafe_allow_html=True,
        )
        fig = create_trend_chart(df_vendas)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")

    # Grid de gráficos
    col1, col2 = st.columns(2)
    with col1:
        if mostrar_categorias and not df_categorias.empty:
            st.markdown(
                '<h2 class="section-title">🏷️ Performance por Categoria</h2>',
                unsafe_allow_html=True,
            )
            fig = create_category_chart(df_categorias)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if mostrar_regioes and not df_regioes.empty:
            st.markdown(
                '<h2 class="section-title">🌎 Vendas por Região</h2>',
                unsafe_allow_html=True,
            )
            fig = create_pie_chart(
                df_regioes.head(5),
                "receita_total",
                "estado",
                "Distribuição Regional",
            )
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Top produtos
    if mostrar_produtos and not df_produtos.empty:
        st.markdown(
            '<h2 class="section-title">⭐ Top 10 Produtos</h2>',
            unsafe_allow_html=True,
        )

        df_produtos_display = df_produtos.head(10).copy()
        df_produtos_display["receita_total_fmt"] = df_produtos_display[
            "receita_total"
        ].apply(format_currency)
        df_produtos_display["giro_estoque_fmt"] = df_produtos_display[
            "giro_estoque"
        ].apply(lambda x: f"{x:.2f}x")

        cols = st.columns(4)
        for i, (idx, row) in enumerate(df_produtos_display.iterrows(), start=1):
            with cols[(i - 1) % 4]:
                st.markdown(
                    f"""
                    <div class="metric-card" style="padding: 15px; text-align: left;">
                        <p style="font-weight:700; color:#0A2472; font-size:18px; margin-bottom:px;">
                            {i}. {row["nome_produto"][:30]}
                        </p>
                        <p style="display:flex; justify-content:space-between; font-size:18px; margin:0;">
                            <span>💰 {row["receita_total_fmt"]}</span>
                            <span>📦 {int(row["unidades_vendidas"])}</span>
                            <span>🔄 {row["giro_estoque_fmt"]}</span>
                        </p>
                    </div>
                """,
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # Análise de canais
    if mostrar_canais and not df_canais.empty:
        st.markdown(
            '<h2 class="section-title">📱 Performance por Canal</h2>',
            unsafe_allow_html=True,
        )

    fig = go.Figure()

    # Normalizar os valores para melhor visualização
    max_receita = df_canais["receita_total"].max()
    df_canais["receita_normalizada"] = (df_canais["receita_total"] / max_receita) * 100

    fig.add_trace(
        go.Bar(
            name="Receita (%)",
            x=df_canais["canal_venda"],
            y=df_canais["receita_normalizada"],
            text=df_canais["receita_total"].apply(
                lambda x: f"R$ {x:,.2f}".replace(",", "X")
                .replace(".", ",")
                .replace("X", "."),
            ),
            textposition="auto",
            insidetextanchor="middle",
            textfont=dict(
                size=18,
                color="white",
            ),
            marker_color="#715cba",
            opacity=0.8,
        ),
    )

    fig.add_trace(
        go.Scatter(
            name="Pedidos",
            x=df_canais["canal_venda"],
            y=df_canais["total_pedidos"],
            mode="lines+markers+text",
            text=df_canais["total_pedidos"],
            textposition="bottom center",
            line=dict(color="#A23B72", width=3),
            marker=dict(size=10),
            yaxis="y2",
            textfont=dict(
                size=15,
                color="white",
            ),
        ),
    )

    fig.update_layout(
        title="Comparativo: Receita vs Pedidos",
        yaxis=dict(
            title="% da Receita",
            range=[0, 110],
        ),
        yaxis2=dict(
            title="Número de Pedidos",
            side="right",
            overlaying="y",
            range=[0, 85],
            showgrid=False,
        ),
        height=500,
        template="plotly_white",
    )

    st.plotly_chart(fig, use_container_width=True)


# ============================================
# PÁGINA 2: ANÁLISE RFV
# ============================================
def pagina_rfv():
    st.markdown(
        '<h1 class="main-header-2">👥 Análise RFV - Segmentação de Clientes</h1>',
        unsafe_allow_html=True,
    )

    # Sidebar específica para RFV
    with st.sidebar:
        st.markdown("## 🎯 Filtros RFV")

        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input(
                "📅 Período Início",
                value=datetime.now() - timedelta(days=365),
                max_value=datetime.now(),
                key="rfv_inicio",
            )
        with col2:
            data_fim = st.date_input(
                "📅 Período Fim",
                value=datetime.now(),
                max_value=datetime.now(),
                min_value=data_inicio,
                key="rfv_fim",
            )

        segmentos_rfv = [
            "Todos",
            "🏆 Campeões",
            "💎 Clientes VIP",
            "⭐ Clientes Fieis",
            "📈 Potencial Crescimento",
            "⚠️ Em Risco",
            "😴 Hibernando",
            "🆕 Novo Cliente",
            "🔁 Uma Compra",
            "🌱 Cliente Inicial",
        ]
        segmento_filter = st.selectbox("Segmento", segmentos_rfv, key="rfv_segmento")

        st.markdown("### 📊 Visualizações")

        mostrar_segmentos = st.checkbox(
            "Análise por Segmento",
            True,
            key="rfv_segmentos",
        )

        n_clientes = st.slider("Clientes por página", 10, 500, 100, key="rfv_n")

        st.markdown("---")
        st.markdown(f"**Atualização:** {datetime.now().strftime('%H:%M:%S')}")

    # Converter datas
    data_inicio_str = data_inicio.strftime("%Y-%m-%d")
    data_fim_str = data_fim.strftime("%Y-%m-%d")

    # Carregar dados
    with st.spinner("🔄 Calculando métricas RFV..."):
        df_rfv = load_rfv_analysis(data_inicio_str, data_fim_str, segmento_filter)

    if df_rfv.empty:
        st.error("❌ Nenhum dado RFV encontrado para o período selecionado")
        return

    # KPIs RFV
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-label">👥 TOTAL CLIENTES</div>
                <div class="kpi-value">{format_number(len(df_rfv))}</div>
                <div class="kpi-trend positive-trend">
                    {len(df_rfv[df_rfv["frequencia_compras"] > 1])} recorrentes
                </div>
            </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        receita_total = df_rfv["valor_total_gasto"].sum()
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-label">💰 RECEITA TOTAL</div>
                <div class="kpi-value">{format_currency(receita_total)}</div>
                <div class="kpi-trend positive-trend">
                    Média: {format_currency(df_rfv["valor_total_gasto"].mean())}
                </div>
            </div>
        """,
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-label">🎯 TICKET MÉDIO</div>
                <div class="kpi-value">{format_currency(df_rfv["ticket_medio"].mean())}</div>
                <div class="kpi-trend positive-trend">
                    Frequência: {df_rfv["frequencia_compras"].mean():.1f}
                </div>
            </div>
        """,
            unsafe_allow_html=True,
        )

    with col4:
        clientes_top = len(
            df_rfv[df_rfv["segmento_cliente"].str.contains("Campeões|VIP", na=False)],
        )
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-label">🏆 CLIENTES TOP</div>
                <div class="kpi-value">{clientes_top}</div>
                <div class="kpi-trend positive-trend">
                    {clientes_top / len(df_rfv) * 100:.1f}% do total
                </div>
            </div>
        """,
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # Gráficos de segmentos
    if mostrar_segmentos:
        col1, col2 = st.columns([25, 2])
        with col1:
            fig_dist = create_segment_distribution_chart(df_rfv)
            st.plotly_chart(fig_dist, use_container_width=True)

        SCORE_COLORS = {
            "Recencia": "#715cba",
            "Frequencia": "#8572C6",
            "Valor": "#9988D2",
        }

        with col1:
            scores_segmento = (
                df_rfv.groupby("segmento_cliente")[
                    ["score_recencia", "score_frequencia", "score_valor"]
                ]
                .mean()
                .round(2)
            )

            fig_scores = go.Figure()

            for col in scores_segmento.columns:
                nome = col.replace("score_", "").capitalize()

                fig_scores.add_trace(
                    go.Bar(
                        name=nome,
                        x=scores_segmento.index,
                        y=scores_segmento[col],
                        text=scores_segmento[col],
                        textposition="outside",
                        marker=dict(color=SCORE_COLORS.get(nome, "#FFFFFF")),
                        textfont=dict(color="white", size=12, family="Arial"),
                    ),
                )

            fig_scores.update_layout(
                title=dict(
                    text="Média de Scores por Segmento",
                    font=dict(color="white", size=18, family="Arial"),
                ),
                barmode="group",
                height=620,
                xaxis=dict(
                    tickangle=0,
                    tickfont=dict(color="white", size=12, family="Arial"),
                ),
                yaxis=dict(
                    title="Score (1–5)",
                    tickfont=dict(color="white"),
                    range=[0, 5.2],
                    gridcolor="rgba(255,255,255,0.1)",
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.05,
                    xanchor="center",
                    x=0.5,
                    font=dict(color="white"),
                ),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )

    st.plotly_chart(fig_scores, use_container_width=True)

    st.markdown("---")

    # Tabela de clientes
    st.markdown(
        '<h2 class="section-title">📋 Lista de Clientes</h2>',
        unsafe_allow_html=True,
    )

    ordenar_por = st.selectbox(
        "Ordenar por",
        ["Valor Total", "Frequência", "Recência", "Score RFV"],
        key="ordenar_rfv",
    )

    if ordenar_por == "Valor Total":
        df_display = df_rfv.sort_values("valor_total_gasto", ascending=False)
    elif ordenar_por == "Frequência":
        df_display = df_rfv.sort_values("frequencia_compras", ascending=False)
    elif ordenar_por == "Recência":
        df_display = df_rfv.sort_values("dias_ultima_compra", ascending=True)
    else:
        df_display = df_rfv.sort_values("rfv_score_total", ascending=False)

    df_table = (
        df_display[
            [
                "nome_completo",
                "email",
                "cidade",
                "estado",
                "segmento_cliente",
                "dias_ultima_compra",
                "frequencia_compras",
                "valor_total_gasto",
            ]
        ]
        .head(n_clientes)
        .copy()
    )

    df_table["valor_total_gasto"] = df_table["valor_total_gasto"].apply(format_currency)
    df_table["dias_ultima_compra"] = (
        df_table["dias_ultima_compra"].fillna(0).astype(int)
    )
    df_table.columns = [
        "Cliente",
        "Email",
        "Cidade",
        "UF",
        "Segmento",
        "Dias",
        "Freq",
        "Valor Total",
    ]

    st.dataframe(df_table, use_container_width=True, hide_index=True)
    st.markdown("---")


# ============================================
# PÁGINA 3: MÉTRICAS DE ENTREGA
# ============================================
def pagina_entregas():
    st.markdown(
        '<h1 class="main-header-2">📦 Métricas de Entrega e Logística</h1>',
        unsafe_allow_html=True,
    )

    # Sidebar específica para entregas
    with st.sidebar:
        st.markdown("## 🚚 Filtros de Logística")

        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input(
                "📅 Data Inicial",
                value=datetime.now() - timedelta(days=90),
                max_value=datetime.now(),
                key="entrega_inicio",
            )
        with col2:
            data_fim = st.date_input(
                "📅 Data Final",
                value=datetime.now(),
                max_value=datetime.now(),
                min_value=data_inicio,
                key="entrega_fim",
            )

        st.markdown("### 📊 Visualizações")
        mostrar_tempo_medio = st.checkbox(
            "Tempo Médio de Entrega",
            True,
            key="entrega_tempo",
        )
        mostrar_status = st.checkbox("Status das Entregas", True, key="entrega_status")
        mostrar_regioes = st.checkbox(
            "Entregas por Região",
            True,
            key="entrega_regioes",
        )
        mostrar_tendencia = st.checkbox(
            "Tendência de Entregas",
            True,
            key="entrega_tendencia",
        )

        st.markdown("---")
        st.markdown(f"**Atualização:** {datetime.now().strftime('%H:%M:%S')}")

    # Converter datas
    data_inicio_str = data_inicio.strftime("%Y-%m-%d")
    data_fim_str = data_fim.strftime("%Y-%m-%d")

    # Carregar dados
    with st.spinner("Carregando métricas de entrega..."):
        df_tempo = load_tempo_entrega(data_inicio_str, data_fim_str)
        df_status = load_status(data_inicio_str, data_fim_str)
        df_regioes = load_region(data_inicio_str, data_fim_str)
        df_timeline = load_timeline(data_inicio_str, data_fim_str)

    # ============================================
    # CARDS PRINCIPAIS DE ENTREGA
    # ============================================
    if mostrar_tempo_medio and not df_tempo.empty:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            tempo_medio = (
                df_tempo["dias_medios_entrega"].iloc[0] if not df_tempo.empty else 0
            )
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="delivery-label">⏱️ TEMPO MÉDIO</div>
                    <div class="delivery-value">{tempo_medio:.1f} dias</div>
                    <div>por entrega</div>
                </div>
            """,
                unsafe_allow_html=True,
            )

        with col2:
            total_entregas = (
                df_tempo["total_entregas"].iloc[0] if not df_tempo.empty else 0
            )
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="delivery-label">📦 TOTAL ENTREGAS</div>
                    <div class="delivery-value">{format_number(total_entregas)}</div>
                    <div>no período</div>
                </div>
            """,
                unsafe_allow_html=True,
            )

    # ============================================
    # STATUS DAS ENTREGAS
    # ============================================
    if mostrar_status and not df_status.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig_status = px.pie(
                df_status,
                values="quantidade",
                names="status_entrega",
                title="Distribuição por Status de Entrega",
                color_discrete_sequence=["#715cba", "#9988d2"],
                hole=0.4,
            )
            fig_status.update_traces(
                textposition="inside",
                textinfo="percent+label",
                hovertemplate="<b>%{label}</b><br>Quantidade: %{value}<br>Percentual: %{percent}<extra></extra>",
            )
            fig_status.update_layout(height=500)
            st.plotly_chart(fig_status, use_container_width=True)

        with col2:
            fig_status_bar = go.Figure(
                data=[
                    go.Bar(
                        x=df_status["status_entrega"],
                        y=df_status["tempo_medio"],
                        text=df_status["tempo_medio"].apply(lambda x: f"{x:.1f} dias"),
                        textposition="outside",
                        marker_color="#715cba",
                    ),
                ],
            )
            fig_status_bar.update_layout(
                title="Tempo Médio por Status",
                xaxis_title="Status",
                yaxis_title="Dias",
                height=500,
            )
            st.plotly_chart(fig_status_bar, use_container_width=True)

        st.markdown("---")

    # ============================================
    # ENTREGAS POR REGIÃO
    # ============================================
    if mostrar_regioes and not df_regioes.empty:
        st.markdown(
            '<h2 class="section-title">🌎 Performance por Região</h2>',
            unsafe_allow_html=True,
        )

        col1, col2 = st.columns(2)

        with col1:
            fig_regiao = go.Figure(
                data=[
                    go.Bar(
                        x=df_regioes.head(10)["estado"],
                        y=df_regioes.head(10)["total_entregas"],
                        text=df_regioes.head(10)["total_entregas"],
                        textposition="outside",
                        marker_color="#715cba",
                        name="Total Entregas",
                    ),
                ],
            )
            fig_regiao.update_layout(
                title="Top 10 Estados por Volume de Entregas",
                xaxis_title="Estado",
                yaxis_title="Número de Entregas",
                height=500,
            )
            st.plotly_chart(fig_regiao, use_container_width=True)

        with col2:
            fig_regiao_tempo = go.Figure(
                data=[
                    go.Bar(
                        x=df_regioes.head(10)["estado"],
                        y=df_regioes.head(10)["tempo_medio"],
                        text=df_regioes.head(10)["tempo_medio"].apply(
                            lambda x: f"{x:.1f} dias",
                        ),
                        textposition="outside",
                        marker_color="#715cba",
                        name="Tempo Médio",
                    ),
                ],
            )
            fig_regiao_tempo.update_layout(
                title="Tempo Médio de Entrega por Estado",
                xaxis_title="Estado",
                yaxis_title="Dias",
                height=500,
            )
            st.plotly_chart(fig_regiao_tempo, use_container_width=True)

        st.markdown("---")

    # ============================================
    # TENDÊNCIA DE ENTREGAS
    # ============================================
    if mostrar_tendencia and not df_timeline.empty:
        st.markdown(
            '<h2 class="section-title">📈 Tendência de Entregas</h2>',
            unsafe_allow_html=True,
        )

        fig_timeline = make_subplots(specs=[[{"secondary_y": True}]])

        fig_timeline.add_trace(
            go.Scatter(
                x=df_timeline["data_pedido"],
                y=df_timeline["dias_entrega"],
                name="Tempo Médio",
                line=dict(color="#715cba", width=3),
                mode="lines+markers",
            ),
            secondary_y=False,
        )

        fig_timeline.add_trace(
            go.Bar(
                x=df_timeline["data_pedido"],
                y=df_timeline["total_entregas_dia"],
                name="Volume de Entregas",
                marker_color="#24475e",
                opacity=0.6,
            ),
            secondary_y=True,
        )

        fig_timeline.update_layout(
            title="Evolução do Tempo e Volume de Entregas",
            hovermode="x unified",
            height=450,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5,
            ),
        )

        fig_timeline.update_yaxes(title_text="Dias para Entrega", secondary_y=False)
        fig_timeline.update_yaxes(title_text="Número de Entregas", secondary_y=True)

        st.plotly_chart(fig_timeline, use_container_width=True)

        st.markdown("---")

    # ============================================
    # TABELA DETALHADA
    # ============================================
    st.markdown(
        '<h2 class="section-title">📋 Detalhamento de Entregas por Região</h2>',
        unsafe_allow_html=True,
    )

    df_display = df_regioes.copy()
    df_display["tempo_medio"] = df_display["tempo_medio"].apply(
        lambda x: f"{x:.1f} dias",
    )
    df_display["taxa_rapida"] = df_display["taxa_rapida"].apply(lambda x: f"{x:.1f}%")
    df_display["frete_medio"] = df_display["frete_medio"].apply(
        lambda x: format_currency(x),
    )
    df_display.columns = [
        "Estado",
        "Total Entregas",
        "Tempo Médio",
        "Taxa Rápida (≤3d)",
        "Frete Médio",
    ]

    st.dataframe(df_display, use_container_width=True, hide_index=True)


# ============================================
# MENU PRINCIPAL
# ============================================
def main():
    # Configurações da página
    st.set_page_config(
        page_title="DataLake Analytics - Executive Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Header
    st.markdown(
        '<h1 class="main-header" style="text-align: center">👟 DataLake Analytics - Dashboard Executivo</h1>',
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.image("image/Logo.png", use_container_width=True)

    # Menu de navegação no topo
    pagina = st.sidebar.selectbox(
        "📌 Navegação",
        ["📊 Vendas", "👥 RFV Clientes", "📦 Entregas"],
        key="navegacao",
    )

    # Navegação entre páginas
    if pagina == "📊 Vendas":
        pagina_vendas()
    elif pagina == "👥 RFV Clientes":
        pagina_rfv()
    elif pagina == "📦 Entregas":
        pagina_entregas()

    # Logo e info na sidebar
    with st.sidebar:
        st.markdown("---")
        st.markdown("### ℹ️ Informações")
        st.markdown("**Lakehouse:** Databricks")
        st.markdown("**Camada:** Gold/Silver")
        st.markdown("**Conexão:** 🟢 Ativa")

        # Botão de refresh global
        if st.button("🔄 Atualizar Todos os Dados", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # Footer
    st.markdown(
        """
        <div class="footer">
            <p>🏢 DataLake Analytics - Powered by Databricks Lakehouse</p>
            <p>© 2026 - Dados em tempo real das camadas silver e gold</p>
        </div>
    """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
