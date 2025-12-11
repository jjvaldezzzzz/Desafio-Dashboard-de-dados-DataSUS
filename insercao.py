import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import warnings
import os
from dotenv import load_dotenv
# Ignorar warnings de data e pandas
warnings.filterwarnings("ignore")

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

CSV_FILE = 'sus.csv'

CONN_STR = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(CONN_STR)

# Mapa Oficial IBGE
MAPA_UFS = {
    11: ('Rondônia', 'RO'), 12: ('Acre', 'AC'), 13: ('Amazonas', 'AM'), 14: ('Roraima', 'RR'), 15: ('Pará', 'PA'),
    16: ('Amapá', 'AP'), 17: ('Tocantins', 'TO'), 21: ('Maranhão', 'MA'), 22: ('Piauí', 'PI'), 23: ('Ceará', 'CE'),
    24: ('Rio Grande do Norte', 'RN'), 25: ('Paraíba', 'PB'), 26: ('Pernambuco', 'PE'), 27: ('Alagoas', 'AL'),
    28: ('Sergipe', 'SE'), 29: ('Bahia', 'BA'), 31: ('Minas Gerais', 'MG'), 32: ('Espírito Santo', 'ES'),
    33: ('Rio de Janeiro', 'RJ'), 35: ('São Paulo', 'SP'), 41: ('Paraná', 'PR'), 42: ('Santa Catarina', 'SC'),
    43: ('Rio Grande do Sul', 'RS'), 50: ('Mato Grosso do Sul', 'MS'), 51: ('Mato Grosso', 'MT'),
    52: ('Goiás', 'GO'), 53: ('Distrito Federal', 'DF')
}

# Funções de Limpeza
def clean_boolean(val):
    if pd.isna(val): return None
    s = str(val).upper().strip()
    return True if s in ['TRUE', 'VERDADEIRO', 'SIM', 'S', '1'] else False if s in ['FALSE', 'FALSO', 'NAO', 'NÃO', 'N', '0'] else None

def clean_code(value):
    try:
        if pd.isna(value) or value == '': return None
        return str(int(float(value)))
    except:
        return str(value)

print(">> 1. Lendo CSV...")
df = pd.read_csv(CSV_FILE, sep=',', encoding='utf-8', low_memory=False)
df['notificacao_id'] = df.index + 1

# Tratamento de Datas
date_cols = ['dataNotificacao', 'dataInicioSintomas', 'dataEncerramento', 
             'dataColetaTeste1', 'dataColetaTeste2', 'dataColetaTeste3', 'dataColetaTeste4', 
             'dataPrimeiraDose', 'dataSegundaDose']
for col in date_cols:
    df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date

# ==============================================================================
# 3. GEOGRAFIA INTELIGENTE (COM RECUPERAÇÃO DE DADOS)
# ==============================================================================
print(">> 3. Processando Geografia (Com Recuperação Inteligente)...")

# --- PASSO A: CRIAR DICIONÁRIO DE CORREÇÃO ---
# Vamos varrer o dataset inteiro procurando associações Nome -> Código que existam
# para preencher os buracos (NaN) onde só temos o nome.

# 1. Mapeamento de Residência
ref_residencia = df[['municipio', 'municipioIBGE']].dropna().drop_duplicates('municipio')
ref_residencia['municipioIBGE'] = pd.to_numeric(ref_residencia['municipioIBGE'], errors='coerce')
mapa_nomes_res = dict(zip(ref_residencia['municipio'], ref_residencia['municipioIBGE']))

# 2. Mapeamento de Notificação
ref_notificacao = df[['municipioNotificacao', 'municipioNotificacaoIBGE']].dropna().drop_duplicates('municipioNotificacao')
ref_notificacao['municipioNotificacaoIBGE'] = pd.to_numeric(ref_notificacao['municipioNotificacaoIBGE'], errors='coerce')
mapa_nomes_not = dict(zip(ref_notificacao['municipioNotificacao'], ref_notificacao['municipioNotificacaoIBGE']))

# Fundir os conhecimentos (Notificação costuma ser mais confiavel para grafia)
mapa_geral = {**mapa_nomes_res, **mapa_nomes_not}

print(f"   -> Dicionário de recuperação criado com {len(mapa_geral)} cidades conhecidas.")

# --- PASSO B: APLICAR CORREÇÃO NOS DADOS ORIGINAIS ---

# Função para preencher buracos
def preencher_ibge(row, col_ibge, col_nome):
    valor_atual = row[col_ibge]
    # Se já é um número válido, retorna ele
    try:
        if float(valor_atual) > 99999: return float(valor_atual)
    except:
        pass
    
    # Se está vazio, tenta achar pelo nome
    nome = row[col_nome]
    if pd.notna(nome) and nome in mapa_geral:
        return mapa_geral[nome]
    
    return valor_atual # Se não achou, desiste

# Aplicando a correção (Isso vai salvar o Tucuruí sem código!)
print("   -> Aplicando correção nos IDs nulos...")
df['municipioIBGE'] = df.apply(lambda row: preencher_ibge(row, 'municipioIBGE', 'municipio'), axis=1)
df['municipioNotificacaoIBGE'] = df.apply(lambda row: preencher_ibge(row, 'municipioNotificacaoIBGE', 'municipioNotificacao'), axis=1)

# --- PASSO C: INSERÇÃO NORMAL (AGORA COM DADOS RECUPERADOS) ---

# Inserir Estados
df_estado_clean = pd.DataFrame([{'estado_ibge': k, 'nome': v[0], 'sigla': v[1]} for k, v in MAPA_UFS.items()])
df_estado_clean.to_sql('estado', engine, if_exists='append', index=False, method='multi')

# Preparar Municípios para Inserção (União Residência + Notificação)
df_mun1 = df[['municipioIBGE', 'municipio']].rename(columns={'municipioIBGE': 'id', 'municipio': 'nome'})
df_mun2 = df[['municipioNotificacaoIBGE', 'municipioNotificacao']].rename(columns={'municipioNotificacaoIBGE': 'id', 'municipioNotificacao': 'nome'})

df_mun_total = pd.concat([df_mun1, df_mun2]).drop_duplicates('id')
df_mun_total['id'] = pd.to_numeric(df_mun_total['id'], errors='coerce').fillna(0).astype(int)
df_mun_total['estado_ibge'] = df_mun_total['id'].apply(lambda x: int(str(x)[:2]) if x > 99999 else None)

# Filtra apenas válidos e insere
df_mun_final = df_mun_total[(df_mun_total['id'] > 99999) & (df_mun_total['estado_ibge'].isin(MAPA_UFS.keys()))]
df_mun_final = df_mun_final.rename(columns={'id': 'municipio_ibge'})[['municipio_ibge', 'nome', 'estado_ibge']]

print(f"   -> Inserindo {len(df_mun_final)} municípios...")
df_mun_final.to_sql('municipio', engine, if_exists='append', index=False, method='multi', chunksize=1000)
# ==============================================================================
# 4. NOTIFICAÇÃO
# ==============================================================================
print(">> 4. Inserindo Notificações...")
df_not = df.copy()
df_not['municipio_notificacao_ibge'] = pd.to_numeric(df_not['municipioNotificacaoIBGE'], errors='coerce').fillna(0).astype(int)
df_not['estado_notificacao_ibge'] = df_not['municipio_notificacao_ibge'].apply(lambda x: int(str(x)[:2]) if x > 99999 else None)
df_not['excluido'] = df_not['excluido'].apply(clean_boolean)
df_not['validado'] = df_not['validado'].apply(clean_boolean)

# Filtro de segurança (FK)
valid_mun_ids = set(df_mun_final['municipio_ibge'])
df_insert_not = df_not[df_not['municipio_notificacao_ibge'].isin(valid_mun_ids)].copy()

cols_not = {
    'notificacao_id': 'notificacao_id', 'source_id': 'source_id',
    'dataNotificacao': 'data_notificacao', 'municipio_notificacao_ibge': 'municipio_notificacao_ibge',
    'estado_notificacao_ibge': 'estado_notificacao_ibge', 'excluido': 'excluido', 'validado': 'validado'
}
df_insert_not = df_insert_not[list(cols_not.keys())].rename(columns=cols_not)
df_insert_not.to_sql('notificacao', engine, if_exists='append', index=False, method='multi', chunksize=2000)

# Atualizar DF base
df = df[df['notificacao_id'].isin(df_insert_not['notificacao_id'])]

# ==============================================================================
# 5. TABELAS SATÉLITES (AGORA COMPLETAS)
# ==============================================================================
print(">> 5. Inserindo Satélites (Demográfico, Clínico, Gestão, Epidemio)...")

# 5.1 Demográficos
cols_demo = {'notificacao_id': 'notificacao_id', 'idade': 'idade', 'sexo': 'sexo', 'racaCor': 'raca_cor',
    'profissionalSaude': 'is_profissional_saude', 'profissionalSeguranca': 'is_profissional_seguranca', 'cbo': 'cbo', 
    'codigoContemComunidadeTradicional': 'pertence_comunidade_tradicional'}
temp_demo = df[list(cols_demo.keys())].rename(columns=cols_demo)
temp_demo['pertence_comunidade_tradicional'] = temp_demo['pertence_comunidade_tradicional'].apply(clean_code) # Limpar código .0
temp_demo['pertence_comunidade_tradicional'] = temp_demo['pertence_comunidade_tradicional'].apply(lambda x: True if x == '2' else False if x == '1' else None) # Exemplo de conversão se necessário, ou deixe clean_boolean
temp_demo.to_sql('dados_demograficos', engine, if_exists='append', index=False, method='multi', chunksize=2000)

# 5.2 Clínicos
cols_clin = {'notificacao_id': 'notificacao_id', 'dataInicioSintomas': 'data_inicio_sintomas', 'dataEncerramento': 'data_encerramento',
    'classificacaoFinal': 'classificacao_final', 'evolucaoCaso': 'evolucao_caso', 'totalTestesRealizados': 'total_testes_realizados',
    'outrosSintomas': 'outros_sintomas', 'outrasCondicoes': 'outras_condicoes'}
df[list(cols_clin.keys())].rename(columns=cols_clin).to_sql('dados_clinicos', engine, if_exists='append', index=False, method='multi', chunksize=2000)

# 5.3 Gestão e Estratégia (QUE ESTAVA FALTANDO)
cols_gestao = {
    'notificacao_id': 'notificacao_id',
    'codigoEstrategiaCovid': 'codigo_estrategia_covid',
    'codigoBuscaAtivaAssintomatico': 'codigo_busca_ativa_assintomatico',
    'outroBuscaAtivaAssintomatico': 'outro_busca_ativa_assintomatico',
    'codigoTriagemPopulacaoEspecifica': 'codigo_triagem_populacao_especifica',
    'outroTriagemPopulacaoEspecifica': 'outro_triagem_populacao_especifica',
    'codigoLocalRealizacaoTestagem': 'codigo_local_realizacao_testagem',
    'outroLocalRealizacaoTestagem': 'outro_local_realizacao_testagem'
}
temp_gestao = df[list(cols_gestao.keys())].rename(columns=cols_gestao)
# Limpa os códigos numéricos (ex: 1.0 -> 1)
for c in ['codigo_estrategia_covid', 'codigo_busca_ativa_assintomatico', 'codigo_triagem_populacao_especifica', 'codigo_local_realizacao_testagem']:
    temp_gestao[c] = temp_gestao[c].apply(clean_code)
temp_gestao.to_sql('dados_gestao_estrategia', engine, if_exists='append', index=False, method='multi', chunksize=2000)

# 5.4 Epidemiológicos (QUE TAMBÉM ESTAVA FALTANDO)
# Nota: Aqui precisamos garantir que municipio_residencia exista na tabela municipio.
# Como fizemos a união antes, deve estar lá. Mas por segurança, filtramos.
cols_epi = {
    'notificacao_id': 'notificacao_id',
    'origem': 'origem_dados',
    'municipioIBGE': 'municipio_residencia_ibge',
    # estado_residencia_ibge derivamos abaixo
}
temp_epi = df.copy()
temp_epi['municipio_residencia_ibge'] = pd.to_numeric(temp_epi['municipioIBGE'], errors='coerce').fillna(0).astype(int)
temp_epi['estado_residencia_ibge'] = temp_epi['municipio_residencia_ibge'].apply(lambda x: int(str(x)[:2]) if x > 99999 else None)

# Filtra residência inválida para não quebrar FK (se residência for nula, inserimos nulo no banco)
temp_epi.loc[~temp_epi['municipio_residencia_ibge'].isin(valid_mun_ids), 'municipio_residencia_ibge'] = None
temp_epi.loc[temp_epi['municipio_residencia_ibge'].isnull(), 'estado_residencia_ibge'] = None

final_epi = temp_epi[['notificacao_id', 'origem', 'municipio_residencia_ibge', 'estado_residencia_ibge']].rename(columns={'origem': 'origem_dados'})
final_epi.to_sql('dados_epidemiologicos', engine, if_exists='append', index=False, method='multi', chunksize=2000)

# ==============================================================================
# 6. SINTOMAS
# ==============================================================================
print(">> 6. Sintomas...")
df_sint = df[['notificacao_id', 'sintomas']].dropna()
df_sint = df_sint.assign(nome=df_sint['sintomas'].str.split(',')).explode('nome')
df_sint['nome'] = df_sint['nome'].str.strip()
sintomas_unicos = pd.DataFrame(df_sint['nome'].unique(), columns=['nome']).dropna()
sintomas_unicos.to_sql('sintoma', engine, if_exists='append', index=False, method='multi')

db_sintomas = pd.read_sql("SELECT sintoma_id, nome FROM sintoma", engine)
mapa_sintomas = dict(zip(db_sintomas['nome'], db_sintomas['sintoma_id']))
df_sint['sintoma_id'] = df_sint['nome'].map(mapa_sintomas)
df_sint[['notificacao_id', 'sintoma_id']].dropna().drop_duplicates().to_sql('notificacao_sintoma', engine, if_exists='append', index=False, method='multi', chunksize=5000)

# ==============================================================================
# 7. TESTES
# ==============================================================================
print(">> 7. Testes...")
lista_dfs = []
for i in range(1, 5):
    cols = {f'codigoTipoTeste{i}': 'tipo_teste', f'codigoFabricanteTeste{i}': 'fabricante_teste',
        f'codigoResultadoTeste{i}': 'resultado_teste', f'codigoEstadoTeste{i}': 'estado_teste', f'dataColetaTeste{i}': 'data_coleta'}
    temp = df[['notificacao_id'] + list(cols.keys())].rename(columns=cols)
    temp['numero_sequencial'] = i
    temp = temp.dropna(subset=['tipo_teste'])
    for c in ['tipo_teste', 'fabricante_teste', 'resultado_teste', 'estado_teste']:
        temp[c] = temp[c].apply(clean_code)
    lista_dfs.append(temp)

if lista_dfs:
    pd.concat(lista_dfs).to_sql('teste_laboratorial', engine, if_exists='append', index=False, method='multi', chunksize=2000)

# ==============================================================================
# 8. VACINAS (ADICIONADO)
# ==============================================================================
print(">> 8. Vacinas...")
# Pivotando Dose 1 e Dose 2
vacinas_list = []
for i, nome_dose in [(1, 'PrimeiraDose'), (2, 'SegundaDose')]:
    cols = {
        f'data{nome_dose}': 'data_aplicacao',
        f'codigoLaboratorio{nome_dose}': 'laboratorio',
        f'lote{nome_dose}': 'lote'
    }
    temp = df[['notificacao_id'] + list(cols.keys())].rename(columns=cols)
    temp['dose_numero'] = i
    temp = temp.dropna(subset=['data_aplicacao']) # Só insere se tiver data
    vacinas_list.append(temp)

if vacinas_list:
    pd.concat(vacinas_list).to_sql('vacina_aplicada', engine, if_exists='append', index=False, method='multi', chunksize=2000)

print(">> AGORA SIM! TUDO CARREGADO. 🚀")