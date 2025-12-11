import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuração da página
st.set_page_config(
    page_title="Dashboard Epidemiológico - Pará",
    page_icon="🦠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Carregar os dados
@st.cache_data
def load_data():
    df = pd.read_csv('df_padronizado_para_o_dash.csv', sep=';', encoding='utf-8')
    
    # Converter datas
    date_cols = ['data_notificacao', 'data_inicio_sintomas']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Criar coluna de ano-mês para agrupamento
    df['ano_mes'] = df['data_notificacao'].dt.strftime('%Y-%m')
    df['mes_ano'] = df['data_notificacao'].dt.strftime('%m/%Y')
    
    return df

df = load_data()

# Título e descrição
st.title("🦠 Dashboard Epidemiológico - COVID-19 Pará")
st.markdown("""
    Análise interativa dos casos de COVID-19 no estado do Pará. 
    Use os filtros na barra lateral para explorar os dados.
""")

# Sidebar com filtros
st.sidebar.header("🔍 Filtros")

# Filtro por município
municipios = sorted(df['municipio_nome'].dropna().unique())
municipio_selecionado = st.sidebar.multiselect(
    "Municípios", 
    municipios, 
    default=["Tucuruí", "Belém", "Conceição do Araguaia"]
)

# Filtro por período
if not df['data_notificacao'].isnull().all():
    min_date = df['data_notificacao'].min()
    max_date = df['data_notificacao'].max()
    
    date_range = st.sidebar.date_input(
        "Período",
        [min_date.date(), max_date.date()],
        min_value=min_date.date(),
        max_value=max_date.date()
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        df_filtrado = df[
            (df['data_notificacao'].dt.date >= start_date) &
            (df['data_notificacao'].dt.date <= end_date)
        ]
    else:
        df_filtrado = df.copy()
else:
    df_filtrado = df.copy()

# Filtrar por municípios selecionados
if municipio_selecionado:
    df_filtrado = df_filtrado[df_filtrado['municipio_nome'].isin(municipio_selecionado)]

# Layout principal com tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 Visão Geral", 
    "👥 Demografia", 
    "💉 Vacinação", 
    "🧪 Testes", 
    "🗺️ Mapa", 
    "📊 Modelo Preditivo"
])

# TAB 1: VISÃO GERAL
with tab1:
    st.header("Evolução Temporal dos Casos")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_casos = len(df_filtrado)
        st.metric("Total de Notificações", f"{total_casos:,}")
    
    with col2:
        confirmados = df_filtrado['target_confirmado'].sum()
        st.metric("Casos Confirmados", f"{confirmados:,}")
    
    with col3:
        taxa_confirmacao = (confirmados / total_casos * 100) if total_casos > 0 else 0
        st.metric("Taxa de Confirmação", f"{taxa_confirmacao:.1f}%")
    
    # Gráfico de evolução temporal
    fig_temporal = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Casos por Mês', 'Taxa de Confirmação por Mês'),
        vertical_spacing=0.15,
        row_heights=[0.7, 0.3]
    )
    
    # Casos por mês
    casos_mes = df_filtrado.groupby('mes_ano').size().reset_index(name='total')
    confirmados_mes = df_filtrado[df_filtrado['target_confirmado'] == 1].groupby('mes_ano').size().reset_index(name='confirmados')
    
    fig_temporal.add_trace(
        go.Bar(
            x=casos_mes['mes_ano'],
            y=casos_mes['total'],
            name='Total Notificações',
            marker_color='lightblue'
        ),
        row=1, col=1
    )
    
    fig_temporal.add_trace(
        go.Bar(
            x=confirmados_mes['mes_ano'],
            y=confirmados_mes['confirmados'],
            name='Confirmados',
            marker_color='red'
        ),
        row=1, col=1
    )
    
    # Taxa de confirmação por mês
    df_mes = pd.merge(casos_mes, confirmados_mes, on='mes_ano', how='left')
    df_mes['taxa_confirmacao'] = (df_mes['confirmados'] / df_mes['total'] * 100).fillna(0)
    
    fig_temporal.add_trace(
        go.Scatter(
            x=df_mes['mes_ano'],
            y=df_mes['taxa_confirmacao'],
            name='Taxa Confirmação',
            mode='lines+markers',
            line=dict(color='green', width=3),
            yaxis='y2'
        ),
        row=2, col=1
    )
    
    fig_temporal.update_layout(
        height=600,
        showlegend=True,
        xaxis_title="Mês/Ano",
        yaxis_title="Número de Casos",
        yaxis2_title="Taxa (%)",
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_temporal, use_container_width=True)
    
    # Top 10 municípios
    st.subheader("Top 10 Municípios por Número de Casos")
    
    top_municipios = df_filtrado['municipio_nome'].value_counts().head(10)
    
    fig_top = px.bar(
        x=top_municipios.index,
        y=top_municipios.values,
        labels={'x': 'Município', 'y': 'Número de Casos'},
        color=top_municipios.values,
        color_continuous_scale='Viridis'
    )
    
    fig_top.update_layout(
        xaxis_tickangle=-45,
        height=400
    )
    
    st.plotly_chart(fig_top, use_container_width=True)

# TAB 2: DEMOGRAFIA
with tab2:
    st.header("Distribuição Demográfica dos Casos")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribuição por sexo
        st.subheader("📊 Distribuição por Sexo")
        sexo_counts = df_filtrado['sexo'].value_counts()
        
        fig_sexo = px.pie(
            values=sexo_counts.values,
            names=sexo_counts.index,
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        
        st.plotly_chart(fig_sexo, use_container_width=True)
    
    with col2:
        # Distribuição por faixa etária
        st.subheader("👶👨👴 Distribuição por Faixa Etária")
        faixa_counts = df_filtrado['faixa_etaria'].value_counts()
        
        fig_faixa = px.bar(
            x=faixa_counts.index,
            y=faixa_counts.values,
            labels={'x': 'Faixa Etária', 'y': 'Número de Casos'},
            color=faixa_counts.values,
            color_continuous_scale='Blues'
        )
        
        fig_faixa.update_layout(
            xaxis_tickangle=-45,
            height=400
        )
        
        st.plotly_chart(fig_faixa, use_container_width=True)
    
    col3, col4 = st.columns(2)
    
    with col3:
        # Distribuição por raça/cor
        st.subheader("🎨 Distribuição por Raça/Cor")
        raca_counts = df_filtrado['raca_cor'].value_counts().head(10)
        
        fig_raca = px.bar(
            x=raca_counts.index,
            y=raca_counts.values,
            labels={'x': 'Raça/Cor', 'y': 'Número de Casos'},
            color=raca_counts.values,
            color_continuous_scale='Greens'
        )
        
        fig_raca.update_layout(
            xaxis_tickangle=-45,
            height=400
        )
        
        st.plotly_chart(fig_raca, use_container_width=True)
    
    with col4:
        # Distribuição por ocupação
        st.subheader("💼 Distribuição por Ocupação")
        ocupacao_counts = df_filtrado['categoria_ocupacao'].value_counts()
        
        fig_ocupacao = px.pie(
            values=ocupacao_counts.values,
            names=ocupacao_counts.index,
            hole=0.3,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        
        st.plotly_chart(fig_ocupacao, use_container_width=True)

# TAB 3: VACINAÇÃO
with tab3:
    st.header("Análise da Vacinação")
    
    # Status vacinal
    st.subheader("Status Vacinal dos Casos")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        nao_vacinados = len(df_filtrado[df_filtrado['status_vacinal'] == 'Não Vacinado'])
        st.metric("Não Vacinados", nao_vacinados)
    
    with col2:
        esquema_completo = len(df_filtrado[df_filtrado['status_vacinal'] == 'Esquema Completo'])
        st.metric("Esquema Completo", esquema_completo)
    
    with col3:
        parcial = len(df_filtrado[df_filtrado['status_vacinal'] == 'Parcial'])
        st.metric("Parcialmente Vacinados", parcial)
    
    # Gráfico de status vacinal vs confirmação
    status_vacinal_data = df_filtrado.groupby(['status_vacinal', 'target_confirmado']).size().reset_index(name='count')
    
    fig_vacina = px.bar(
        status_vacinal_data,
        x='status_vacinal',
        y='count',
        color='target_confirmado',
        barmode='group',
        labels={
            'status_vacinal': 'Status Vacinal',
            'count': 'Número de Casos',
            'target_confirmado': 'Confirmado'
        },
        color_discrete_map={0: 'blue', 1: 'red'}
    )
    
    fig_vacina.update_layout(
        height=400,
        xaxis_title="Status Vacinal",
        yaxis_title="Número de Casos"
    )
    
    st.plotly_chart(fig_vacina, use_container_width=True)
    
    # Fabricantes de vacina
    st.subheader("Fabricantes de Vacina")
    
    fabricantes_counts = df_filtrado['fabricantes_vacina'].value_counts().head(10)
    
    fig_fabricantes = px.bar(
        x=fabricantes_counts.index,
        y=fabricantes_counts.values,
        labels={'x': 'Fabricante', 'y': 'Número de Casos'},
        color=fabricantes_counts.values,
        color_continuous_scale='Purples'
    )
    
    fig_fabricantes.update_layout(
        xaxis_tickangle=-45,
        height=400
    )
    
    st.plotly_chart(fig_fabricantes, use_container_width=True)
    
    # Taxa de confirmação por status vacinal
    st.subheader("Taxa de Confirmação por Status Vacinal")
    
    taxa_vacina = df_filtrado.groupby('status_vacinal').agg({
        'target_confirmado': ['count', 'sum']
    }).round(2)
    
    taxa_vacina.columns = ['total', 'confirmados']
    taxa_vacina['taxa'] = (taxa_vacina['confirmados'] / taxa_vacina['total'] * 100).round(1)
    taxa_vacina = taxa_vacina.reset_index()
    
    fig_taxa_vacina = px.bar(
        taxa_vacina,
        x='status_vacinal',
        y='taxa',
        text='taxa',
        labels={'status_vacinal': 'Status Vacinal', 'taxa': 'Taxa de Confirmação (%)'},
        color='taxa',
        color_continuous_scale='RdYlGn_r'
    )
    
    fig_taxa_vacina.update_traces(texttemplate='%{text}%', textposition='outside')
    fig_taxa_vacina.update_layout(height=400)
    
    st.plotly_chart(fig_taxa_vacina, use_container_width=True)

# TAB 4: TESTES
with tab4:
    st.header("Análise de Testes")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_testes = df_filtrado['testes_realizados'].sum()
        st.metric("Total de Testes Realizados", total_testes)
    
    with col2:
        positivos = len(df_filtrado[df_filtrado['resultado_teste_agregado'] == 'Positivo'])
        st.metric("Testes Positivos", positivos)
    
    with col3:
        taxa_positividade = (positivos / total_testes * 100) if total_testes > 0 else 0
        st.metric("Taxa de Positividade", f"{taxa_positividade:.1f}%")
    
    # Tipos de teste
    st.subheader("Tipos de Teste Realizados")
    
    # Processar tipos de teste (a coluna contém listas separadas por vírgula)
    tipos_testes = []
    for lista in df_filtrado['tipos_testes_lista'].dropna():
        if ';' in str(lista):
            tipos_testes.extend([t.strip() for t in str(lista).split(';')])
        else:
            tipos_testes.append(str(lista).strip())
    
    tipos_counts = pd.Series(tipos_testes).value_counts().head(10)
    
    fig_testes = px.bar(
        x=tipos_counts.index,
        y=tipos_counts.values,
        labels={'x': 'Tipo de Teste', 'y': 'Quantidade'},
        color=tipos_counts.values,
        color_continuous_scale='Oranges'
    )
    
    fig_testes.update_layout(
        xaxis_tickangle=-45,
        height=400
    )
    
    st.plotly_chart(fig_testes, use_container_width=True)
    
    # Fabricantes de teste
    st.subheader("Fabricantes de Teste")
    
    fabricantes_teste = []
    for lista in df_filtrado['fabricantes_teste_lista'].dropna():
        if ';' in str(lista):
            fabricantes_teste.extend([t.strip() for t in str(lista).split(';')])
        else:
            fabricantes_teste.append(str(lista).strip())
    
    fabricantes_counts = pd.Series(fabricantes_teste).value_counts().head(10)
    
    fig_fab_testes = px.bar(
        x=fabricantes_counts.index,
        y=fabricantes_counts.values,
        labels={'x': 'Fabricante do Teste', 'y': 'Quantidade'},
        color=fabricantes_counts.values,
        color_continuous_scale='Bluered'
    )
    
    fig_fab_testes.update_layout(
        xaxis_tickangle=-45,
        height=400
    )
    
    st.plotly_chart(fig_fab_testes, use_container_width=True)
    
    # Resultados dos testes
    st.subheader("Distribuição dos Resultados dos Testes")
    
    resultados_counts = df_filtrado['resultado_teste_agregado'].value_counts()
    
    fig_resultados = px.pie(
        values=resultados_counts.values,
        names=resultados_counts.index,
        hole=0.3,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    st.plotly_chart(fig_resultados, use_container_width=True)

# TAB 5: MAPA
with tab5:
    st.header("Mapa de Calor - Densidade de Notificações")
    
    # Criar coordenadas aproximadas para municípios (em um cenário real, teríamos lat/long)
    st.warning("🚧 Em desenvolvimento: Em um cenário real, este mapa mostraria a densidade de casos por região com coordenadas geográficas reais.")
    
    # Simulação de dados geográficos para demonstração
    municipio_casos = df_filtrado['municipio_nome'].value_counts().reset_index()
    municipio_casos.columns = ['municipio', 'casos']
    
    # Ordenar por número de casos
    municipio_casos = municipio_casos.sort_values('casos', ascending=False).head(20)
    
    fig_mapa_calor = px.bar(
        municipio_casos,
        x='municipio',
        y='casos',
        title='Top 20 Municípios por Número de Casos',
        labels={'municipio': 'Município', 'casos': 'Número de Casos'},
        color='casos',
        color_continuous_scale='Hot'
    )
    
    fig_mapa_calor.update_layout(
        xaxis_tickangle=-45,
        height=500
    )
    
    st.plotly_chart(fig_mapa_calor, use_container_width=True)
    
    # Gráfico de dispersão sintomas vs confirmação
    st.subheader("Relação entre Sintomas e Confirmação")
    
    sintomas_data = pd.DataFrame({
        'febre': df_filtrado['flg_febre'],
        'tosse': df_filtrado['flg_tosse'],
        'dispneia': df_filtrado['flg_dispneia'],
        'confirmado': df_filtrado['target_confirmado']
    })
    
    # Calcular correlação
    corr_matrix = sintomas_data.corr()
    
    fig_corr = px.imshow(
        corr_matrix,
        text_auto=True,
        aspect="auto",
        color_continuous_scale='RdBu',
        title='Correlação entre Sintomas e Confirmação'
    )
    
    st.plotly_chart(fig_corr, use_container_width=True)

# TAB 6: MODELO PREDITIVO
with tab6:
    st.header("Modelo Preditivo - Probabilidade de Confirmação")
    
    st.info("""
    🔮 **Modelo Estatístico Simulado**
    
    Este modelo estima a probabilidade de um caso ser confirmado com base em:
    - Sintomas apresentados
    - Status vacinal
    - Características demográficas
    - Resultados de testes
    """)
    
    # Interface para simulação de previsão
    st.subheader("Simulador de Probabilidade")
    
    col1, col2 = st.columns(2)
    
    with col1:
        idade = st.slider("Idade", 0, 100, 35)
        sexo = st.selectbox("Sexo", ["Feminino", "Masculino", "Ignorado"])
        status_vacinal = st.selectbox("Status Vacinal", ["Não Vacinado", "Parcial", "Esquema Completo"])
    
    with col2:
        febre = st.checkbox("Febre")
        tosse = st.checkbox("Tosse")
        dispneia = st.checkbox("Dispneia (falta de ar)")
        profissional_saude = st.checkbox("Profissional de Saúde")
    
    # Botão para calcular
    if st.button("Calcular Probabilidade de Confirmação", type="primary"):
        # Simulação de modelo preditivo (em um cenário real, seria um modelo ML treinado)
        
        # Fatores de risco simulados
        probabilidade_base = 0.3  # 30% base
        
        # Ajustes por fatores
        ajustes = 0
        
        if idade >= 60:
            ajustes += 0.15
        elif idade >= 40:
            ajustes += 0.08
        
        if febre:
            ajustes += 0.20
        if tosse:
            ajustes += 0.15
        if dispneia:
            ajustes += 0.25
        
        if status_vacinal == "Esquema Completo":
            ajustes -= 0.20
        elif status_vacinal == "Parcial":
            ajustes -= 0.10
        
        if profissional_saude:
            ajustes += 0.05
        
        # Calcular probabilidade final
        probabilidade_final = min(max(probabilidade_base + ajustes, 0), 0.95)
        probabilidade_percent = probabilidade_final * 100
        
        # Mostrar resultado
        st.subheader(f"📊 Probabilidade Estimada: {probabilidade_percent:.1f}%")
        
        # Barra de progresso
        st.progress(probabilidade_final)
        
        # Interpretação
        if probabilidade_final < 0.3:
            st.success("✅ **Baixa probabilidade** - Menor risco de confirmação")
        elif probabilidade_final < 0.6:
            st.warning("⚠️ **Probabilidade moderada** - Monitorar cuidadosamente")
        else:
            st.error("🔴 **Alta probabilidade** - Maior risco de confirmação")
    
    # Análise de importância dos fatores
    st.subheader("Importância dos Fatores no Modelo")
    
    fatores = pd.DataFrame({
        'Fator': ['Dispneia', 'Febre', 'Idade ≥ 60', 'Tosse', 'Não Vacinado', 'Esquema Vacinal Completo', 'Profissional Saúde'],
        'Impacto': ['+25%', '+20%', '+15%', '+15%', '+0%', '-20%', '+5%'],
        'Direção': ['Aumenta risco', 'Aumenta risco', 'Aumenta risco', 'Aumenta risco', 'Neutro', 'Reduz risco', 'Aumenta risco']
    })
    
    st.dataframe(fatores, use_container_width=True)

# Rodapé
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <p>Dashboard Epidemiológico - COVID-19 Pará | Desenvolvido com Streamlit e Plotly</p>
        <p>Dados: Sistema de Notificação de Síndromes Gripais</p>
    </div>
    """,
    unsafe_allow_html=True
)