-- ============================================================================
-- ETAPA 3.2: CRIAÇÃO DAS VIEWS ANALÍTICAS (PARA O DASHBOARD)
-- ============================================================================

-- 1. VIEW DE CASOS POR MUNICÍPIO (Temporal e Geográfica)
-- Objetivo: Alimentar gráficos de linha (evolução) e mapas.
CREATE OR REPLACE VIEW vw_casos_por_municipio AS
SELECT 
    m.nome AS municipio,
    e.sigla AS uf,
    n.data_notificacao,
    -- Contagem Total
    COUNT(n.notificacao_id) AS total_notificacoes,
    -- Contagem de Confirmados (Filtra por texto padrão do e-SUS)
    COUNT(n.notificacao_id) FILTER (
        WHERE c.classificacao_final ILIKE '%Confirmado%' 
           OR c.classificacao_final ILIKE '%Laboratorial%'
    ) AS casos_confirmados,
    -- Contagem de Descartados
    COUNT(n.notificacao_id) FILTER (
        WHERE c.classificacao_final ILIKE '%Descartado%'
    ) AS casos_descartados,
    -- Contagem de Óbitos
    COUNT(n.notificacao_id) FILTER (
        WHERE c.evolucao_caso ILIKE '%Óbito%' 
           OR c.evolucao_caso ILIKE '%Falecimento%'
    ) AS obitos
FROM notificacao n
JOIN municipio m ON n.municipio_notificacao_ibge = m.municipio_ibge
JOIN estado e ON n.estado_notificacao_ibge = e.estado_ibge
JOIN dados_clinicos c ON n.notificacao_id = c.notificacao_id
WHERE n.excluido = FALSE
GROUP BY m.nome, e.sigla, n.data_notificacao;

-- 2. VIEW DE EFICÁCIA VACINAL (Vacinação vs Desfecho)
-- Objetivo: Mostrar se a vacina reduziu casos graves/óbitos.
CREATE OR REPLACE VIEW vw_vacinacao_por_resultado AS
WITH doses_calculadas AS (
    -- Subquery para contar doses por pessoa antes de agrupar
    SELECT notificacao_id, COUNT(*) as qtd_doses 
    FROM vacina_aplicada 
    GROUP BY notificacao_id
)
SELECT 
    CASE 
        WHEN d.qtd_doses IS NULL OR d.qtd_doses = 0 THEN 'Não Vacinado'
        WHEN d.qtd_doses = 1 THEN 'Parcial (1 Dose)'
        WHEN d.qtd_doses >= 2 THEN 'Esquema Completo'
    END AS status_vacinal,
    -- Trazemos a Classificação e a Evolução para cruzar dados
    COALESCE(c.classificacao_final, 'Em Análise') as classificacao,
    COALESCE(c.evolucao_caso, 'Em Tratamento/Ignorado') as evolucao,
    COUNT(*) AS total_pacientes
FROM notificacao n
JOIN dados_clinicos c ON n.notificacao_id = c.notificacao_id
LEFT JOIN doses_calculadas d ON n.notificacao_id = d.notificacao_id
WHERE n.excluido = FALSE
GROUP BY 1, 2, 3;

-- 3. VIEW DE SINTOMAS (Nuvem de Palavras)
-- Objetivo: Identificar sintomas mais comuns apenas em casos CONFIRMADOS.
CREATE OR REPLACE VIEW vw_sintomas_frequentes AS
SELECT 
    s.nome AS sintoma,
    COUNT(*) AS frequencia_total,
    -- Frequência específica em casos positivos
    COUNT(*) FILTER (
        WHERE c.classificacao_final ILIKE '%Confirmado%' 
           OR c.classificacao_final ILIKE '%Laboratorial%'
    ) AS frequencia_em_confirmados
FROM notificacao_sintoma ns
JOIN sintoma s ON ns.sintoma_id = s.sintoma_id
JOIN dados_clinicos c ON ns.notificacao_id = c.notificacao_id
JOIN notificacao n ON ns.notificacao_id = n.notificacao_id
WHERE n.excluido = FALSE
GROUP BY s.nome
ORDER BY frequencia_em_confirmados DESC;