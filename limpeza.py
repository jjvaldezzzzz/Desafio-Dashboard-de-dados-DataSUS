import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text # <--- IMPORTANTE: 'text' importado aqui
import sys
import os
from dotenv import load_dotenv

# ==============================================================================
# 1. CONFIGURAÇÕES
# ==============================================================================
load_dotenv()

DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'), # <--- Lê do arquivo oculto
    'host': os.getenv('DB_HOST'),
    'port': '5432',
    'dbname': os.getenv('DB_NAME')
}

CONN_STR = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

try:
    engine = create_engine(CONN_STR)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print(">> Conexão com o Banco de Dados: OK")
except Exception as e:
    print(f"\n[ERRO CRÍTICO] Não foi possível conectar ao banco.")
    print(f"Detalhe: {e}")
    sys.exit()

print("\n--- INICIANDO ETAPA 2: GERAÇÃO DE DATASET 'DASHBOARD & ML READY' ---")

# ==============================================================================
# 2. EXTRAÇÃO DOS DADOS (CORRIGIDO PARA SQLALCHEMY 2.0)
# ==============================================================================
print(">> 1. Executando Query SQL complexa...")

# Nota: O uso de : (dois pontos) é para parâmetros. O % é tratado como literal quando usamos text()
query_exportacao = """
/* CTE 1: SINTOMAS */
WITH agg_sintomas AS (
    SELECT 
        ns.notificacao_id,
        STRING_AGG(s.nome, ', ' ORDER BY s.nome) as lista_sintomas,
        MAX(CASE WHEN s.nome ILIKE '%Febre%' THEN 1 ELSE 0 END) as tem_febre,
        MAX(CASE WHEN s.nome ILIKE '%Tosse%' THEN 1 ELSE 0 END) as tem_tosse,
        MAX(CASE WHEN s.nome ILIKE '%Garganta%' THEN 1 ELSE 0 END) as tem_dor_garganta,
        MAX(CASE WHEN s.nome ILIKE '%Dispneia%' OR s.nome ILIKE '%falta de ar%' THEN 1 ELSE 0 END) as tem_dispneia
    FROM notificacao_sintoma ns
    JOIN sintoma s ON ns.sintoma_id = s.sintoma_id
    GROUP BY ns.notificacao_id
),

/* CTE 2: VACINAÇÃO */
agg_vacinas AS (
    SELECT 
        notificacao_id,
        COUNT(*) as total_doses,
        MAX(data_aplicacao) as data_ultima_dose,
        STRING_AGG(DISTINCT laboratorio, ' / ') as fabricantes_vacina
    FROM vacina_aplicada
    GROUP BY notificacao_id
),

/* CTE 3: TESTES LABORATORIAIS */
agg_testes AS (
    SELECT 
        notificacao_id,
        COUNT(*) as qtd_testes_realizados,
        STRING_AGG(DISTINCT tipo_teste, ', ') as tipos_testes_lista,
        STRING_AGG(DISTINCT fabricante_teste, ', ') as fabricantes_teste_lista,
        MAX(CASE WHEN resultado_teste ILIKE '%Positivo%' OR resultado_teste ILIKE '%Detectável%' THEN 1 ELSE 0 END) as houve_teste_positivo,
        MAX(data_coleta) as data_coleta_teste
    FROM teste_laboratorial
    GROUP BY notificacao_id
)

/* QUERY PRINCIPAL */
SELECT 
    n.notificacao_id,
    n.data_notificacao,
    EXTRACT(WEEK FROM n.data_notificacao) as semana_epidemiologica,
    EXTRACT(MONTH FROM n.data_notificacao) as mes_notificacao,
    
    mun.municipio_ibge as codigo_ibge,
    mun.nome as municipio_nome,
    est.sigla as uf_sigla,
    
    d.idade,
    d.sexo,
    d.raca_cor,
    d.cbo as ocupacao_cbo,
    d.is_profissional_saude,
    
    c.data_inicio_sintomas,
    c.classificacao_final,
    c.evolucao_caso,
    
    COALESCE(s.lista_sintomas, 'Assintomático/Não Informado') as sintomas_texto,
    COALESCE(s.tem_febre, 0) as flg_febre,
    COALESCE(s.tem_tosse, 0) as flg_tosse,
    COALESCE(s.tem_dispneia, 0) as flg_dispneia,
    
    COALESCE(v.total_doses, 0) as doses_vacina,
    v.fabricantes_vacina,
    CASE WHEN v.total_doses >= 2 THEN 'Esquema Completo' 
         WHEN v.total_doses = 1 THEN 'Parcial' 
         ELSE 'Não Vacinado' END as status_vacinal,

    COALESCE(t.qtd_testes_realizados, 0) as testes_realizados,
    t.tipos_testes_lista,
    t.fabricantes_teste_lista,
    CASE WHEN t.houve_teste_positivo = 1 THEN 'Positivo' 
         WHEN t.qtd_testes_realizados > 0 THEN 'Negativo/Inconclusivo'
         ELSE 'Não Testado' END as resultado_teste_agregado

FROM notificacao n
INNER JOIN municipio mun ON n.municipio_notificacao_ibge = mun.municipio_ibge
INNER JOIN estado est ON n.estado_notificacao_ibge = est.estado_ibge
INNER JOIN dados_demograficos d ON n.notificacao_id = d.notificacao_id
INNER JOIN dados_clinicos c ON n.notificacao_id = c.notificacao_id
LEFT JOIN agg_sintomas s ON n.notificacao_id = s.notificacao_id
LEFT JOIN agg_vacinas v ON n.notificacao_id = v.notificacao_id
LEFT JOIN agg_testes t ON n.notificacao_id = t.notificacao_id

WHERE n.excluido = FALSE;
"""

try:
    # --- CORREÇÃO AQUI: Usamos engine.connect() e text() ---
    with engine.connect() as conn:
        df_padronizado = pd.read_sql(text(query_exportacao), conn)
    
    if df_padronizado.empty:
        print("\n" + "!"*50)
        print("[ALERTA] O Dataset retornou VAZIO (0 linhas).")
        print("!"*50)
        sys.exit()
        
    print(f" -> Extração concluída. Registros encontrados: {len(df_padronizado)}")

except Exception as e:
    print(f"\n[ERRO SQL] Falha ao executar a consulta.")
    print(f"Detalhe: {e}")
    sys.exit()

# ==============================================================================
# 3. TRATAMENTO E FEATURE ENGINEERING (PYTHON)
# ==============================================================================
print(">> 2. Aplicando regras de negócio e Feature Engineering...")

# 3.1 Tratamento de Outliers de Idade
df_padronizado.loc[(df_padronizado['idade'] < 0) | (df_padronizado['idade'] > 120), 'idade'] = np.nan

# 3.2 Criação de Faixa Etária
bins = [0, 12, 19, 39, 59, 79, 120]
labels = ['Criança (0-12)', 'Adolescente (13-19)', 'Jovem Adulto (20-39)', 'Adulto (40-59)', 'Idoso (60-79)', 'Super Idoso (80+)']
df_padronizado['faixa_etaria'] = pd.cut(df_padronizado['idade'], bins=bins, labels=labels, right=True)

# 3.3 Padronização de Ocupação
df_padronizado['categoria_ocupacao'] = df_padronizado.apply(
    lambda x: 'Profissional de Saúde' if x['is_profissional_saude'] == 'Sim' else 'Outros', axis=1
)

# 3.4 Target para Machine Learning
def definir_target(status):
    if pd.isnull(status): return np.nan
    status = str(status) 
    if 'Confirmado' in status or 'Laboratorial' in status: return 1
    return 0

df_padronizado['target_confirmado'] = df_padronizado['classificacao_final'].apply(definir_target)

# ==============================================================================
# 4. EXPORTAÇÃO E AUDITORIA
# ==============================================================================
arquivo_saida = 'dataset_covid_dashboard_v2.csv'
print(f">> 3. Salvando arquivo final: {arquivo_saida}")
df_padronizado.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8-sig')

# --- Função de Auditoria Rápida ---
def auditar_dataset(df):
    print("\n" + "="*40)
    print("AUDITORIA FINAL (CHECK DE REQUISITOS)")
    print("="*40)
    print(f"1. Total Linhas: {len(df)}")
    print(f"2. Colunas Disponíveis: {list(df.columns)}")
    
    vacinados = df[df['status_vacinal'] != 'Não Vacinado'].shape[0]
    print(f"3. Dados de Vacinação: {vacinados} registros possuem vacina.")
    
    testados = df[df['testes_realizados'] > 0].shape[0]
    print(f"4. Dados Laboratoriais: {testados} registros possuem testes vinculados.")
    
    cidades = df['municipio_nome'].nunique()
    print(f"5. Geografia: Dados abrangem {cidades} municípios distintos.")
    
    targets = df['target_confirmado'].value_counts(normalize=True)
    if not targets.empty:
        print(f"6. Balanceamento do Target (ML): \n{targets.to_string()}")
    else:
        print("6. Balanceamento do Target (ML): [Sem dados suficientes]")

auditar_dataset(df_padronizado)