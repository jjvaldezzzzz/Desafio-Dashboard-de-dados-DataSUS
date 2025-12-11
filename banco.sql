CREATE TABLE IF NOT EXISTS estado (
    estado_ibge INTEGER PRIMARY KEY, 
    nome VARCHAR(100),
    sigla CHAR(2)
);

CREATE TABLE IF NOT EXISTS municipio (
    municipio_ibge INTEGER PRIMARY KEY,
    nome VARCHAR(150),
    estado_ibge INTEGER REFERENCES estado(estado_ibge)
);

CREATE TABLE IF NOT EXISTS notificacao (
    notificacao_id BIGINT PRIMARY KEY, 
    source_id VARCHAR(100),
    data_notificacao DATE,
    municipio_notificacao_ibge INTEGER REFERENCES municipio(municipio_ibge),
    estado_notificacao_ibge INTEGER REFERENCES estado(estado_ibge),
    excluido BOOLEAN DEFAULT FALSE,
    validado BOOLEAN DEFAULT FALSE
);

-- Índices para acelerar filtros de data e local no Dashboard
CREATE INDEX idx_notificacao_data ON notificacao(data_notificacao);
CREATE INDEX idx_notificacao_municipio ON notificacao(municipio_notificacao_ibge);
CREATE INDEX idx_notificacao_source ON notificacao(source_id);


CREATE TABLE IF NOT EXISTS dados_demograficos (
    notificacao_id BIGINT PRIMARY KEY REFERENCES notificacao(notificacao_id) ON DELETE CASCADE,
    idade SMALLINT,
    sexo VARCHAR(20),
    raca_cor VARCHAR(50),
    is_profissional_saude VARCHAR(20),
    is_profissional_seguranca VARCHAR(20),
    cbo VARCHAR(200),
    pertence_comunidade_tradicional BOOLEAN
);

CREATE TABLE IF NOT EXISTS dados_clinicos (
    notificacao_id BIGINT PRIMARY KEY REFERENCES notificacao(notificacao_id) ON DELETE CASCADE,
    data_inicio_sintomas DATE,
    data_encerramento DATE,
    classificacao_final VARCHAR(150),
    evolucao_caso VARCHAR(150),
    outros_sintomas TEXT,
    outras_condicoes TEXT,
    total_testes_realizados INTEGER
);

-- Índice para filtros de tempo de sintomas
CREATE INDEX idx_dados_clinicos_inicio ON dados_clinicos(data_inicio_sintomas);

CREATE TABLE IF NOT EXISTS dados_epidemiologicos (
    notificacao_id BIGINT PRIMARY KEY REFERENCES notificacao(notificacao_id) ON DELETE CASCADE,
    origem_dados VARCHAR(100),
    municipio_residencia_ibge INTEGER REFERENCES municipio(municipio_ibge),
    estado_residencia_ibge INTEGER REFERENCES estado(estado_ibge)
);

CREATE TABLE IF NOT EXISTS dados_gestao_estrategia (
    notificacao_id BIGINT PRIMARY KEY REFERENCES notificacao(notificacao_id) ON DELETE CASCADE,
    codigo_estrategia_covid VARCHAR(100),
    codigo_busca_ativa_assintomatico VARCHAR(100),
    outro_busca_ativa_assintomatico VARCHAR(255),
    codigo_triagem_populacao_especifica VARCHAR(100),
    outro_triagem_populacao_especifica VARCHAR(255),
    codigo_local_realizacao_testagem VARCHAR(100),
    outro_local_realizacao_testagem VARCHAR(255)
);

-- Sintomas
CREATE TABLE IF NOT EXISTS sintoma (
    sintoma_id SERIAL PRIMARY KEY,
    nome VARCHAR(200) UNIQUE
);

CREATE TABLE IF NOT EXISTS notificacao_sintoma (
    notificacao_id BIGINT REFERENCES notificacao(notificacao_id) ON DELETE CASCADE,
    sintoma_id INTEGER REFERENCES sintoma(sintoma_id),
    PRIMARY KEY (notificacao_id, sintoma_id)
);

-- Condições
CREATE TABLE IF NOT EXISTS condicao (
    condicao_id SERIAL PRIMARY KEY,
    nome VARCHAR(200) UNIQUE
);

CREATE TABLE IF NOT EXISTS notificacao_condicao (
    notificacao_id BIGINT REFERENCES notificacao(notificacao_id) ON DELETE CASCADE,
    condicao_id INTEGER REFERENCES condicao(condicao_id),
    PRIMARY KEY (notificacao_id, condicao_id)
);

-- Testes Laboratoriais
CREATE TABLE IF NOT EXISTS teste_laboratorial (
    teste_id SERIAL PRIMARY KEY,
    notificacao_id BIGINT REFERENCES notificacao(notificacao_id) ON DELETE CASCADE,
    numero_sequencial SMALLINT,
    tipo_teste VARCHAR(150),
    fabricante_teste VARCHAR(255),
    resultado_teste VARCHAR(150),
    estado_teste VARCHAR(100),
    data_coleta DATE
);
CREATE INDEX idx_teste_resultado ON teste_laboratorial(resultado_teste);

-- Vacinação
CREATE TABLE IF NOT EXISTS vacina_aplicada (
    vacina_id SERIAL PRIMARY KEY,
    notificacao_id BIGINT REFERENCES notificacao(notificacao_id) ON DELETE CASCADE,
    dose_numero SMALLINT,
    data_aplicacao DATE,
    laboratorio VARCHAR(200),
    lote VARCHAR(100)
);