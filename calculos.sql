CREATE TABLE IF NOT EXISTS indicadores_regionais (
    indicador_id SERIAL PRIMARY KEY,
    municipio_ibge INTEGER REFERENCES municipio(municipio_ibge),
    periodo_inicio DATE,
    periodo_fim DATE,
    data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indicador Solicitado Principal
    taxa_positividade DECIMAL(5,2), 
    
    -- Indicadores Extras (Requisito: Tempo médio, Profissionais Saúde, Vacinas)
    tempo_medio_sintomas_teste DECIMAL(5,1), -- Em dias
    perc_prof_saude_infectados DECIMAL(5,2), -- %
    media_doses_vacina DECIMAL(4,2),         -- Média de doses por pessoa
    
    -- Restrição para garantir que não duplicaremos o cálculo para o mesmo mês/período
    CONSTRAINT uk_indicador_municipio_periodo UNIQUE (municipio_ibge, periodo_inicio, periodo_fim)
);

CREATE OR REPLACE FUNCTION fx_calcular_taxa_positividade(p_inicio DATE, p_fim DATE)
RETURNS VOID AS $$
DECLARE
    reg_municipio RECORD; 
    v_total_testes INT;
    v_total_positivos INT;
    v_taxa_pos DECIMAL(5,2);
    
    -- Variáveis dos outros indicadores
    v_tempo_medio DECIMAL(5,1);
    v_total_confirmados INT;
    v_total_saude_confirmados INT;
    v_perc_saude DECIMAL(5,2);
    v_media_vacina DECIMAL(4,2);

BEGIN
    FOR reg_municipio IN 
        SELECT DISTINCT municipio_notificacao_ibge AS id
        FROM notificacao
        WHERE data_notificacao BETWEEN p_inicio AND p_fim
    LOOP
        
        -- =================================================================
        -- CORREÇÃO AQUI: Mudamos de ILIKE '%Positivo%' para o código '1'
        -- =================================================================
        SELECT 
            COUNT(*), 
            COUNT(*) FILTER (WHERE resultado_teste = '1') -- Código 1 = Positivo
        INTO v_total_testes, v_total_positivos
        FROM teste_laboratorial t
        JOIN notificacao n ON t.notificacao_id = n.notificacao_id
        WHERE n.municipio_notificacao_ibge = reg_municipio.id
          AND n.data_notificacao BETWEEN p_inicio AND p_fim
          AND n.excluido = FALSE;

        IF v_total_testes > 0 THEN
            v_taxa_pos := (v_total_positivos::DECIMAL / v_total_testes::DECIMAL) * 100.0;
        ELSE
            v_taxa_pos := 0.0;
        END IF;

        -- =================================================================
        -- Demais cálculos (Mantidos iguais pois já estão funcionando)
        -- =================================================================
        
        -- Tempo Médio
        SELECT AVG(t.data_coleta - c.data_inicio_sintomas)
        INTO v_tempo_medio
        FROM teste_laboratorial t
        JOIN notificacao n ON t.notificacao_id = n.notificacao_id
        JOIN dados_clinicos c ON n.notificacao_id = c.notificacao_id
        WHERE n.municipio_notificacao_ibge = reg_municipio.id
          AND n.data_notificacao BETWEEN p_inicio AND p_fim
          AND t.data_coleta >= c.data_inicio_sintomas;

        -- % Profissionais Saúde
        SELECT 
            COUNT(*), 
            COUNT(*) FILTER (WHERE d.is_profissional_saude ILIKE 'Sim') 
        INTO v_total_confirmados, v_total_saude_confirmados
        FROM notificacao n
        JOIN dados_clinicos c ON n.notificacao_id = c.notificacao_id
        JOIN dados_demograficos d ON n.notificacao_id = d.notificacao_id
        WHERE n.municipio_notificacao_ibge = reg_municipio.id
          AND n.data_notificacao BETWEEN p_inicio AND p_fim
          AND (c.classificacao_final ILIKE '%Confirmado%' OR c.classificacao_final ILIKE '%Laboratorial%');

        IF v_total_confirmados > 0 THEN
            v_perc_saude := (v_total_saude_confirmados::DECIMAL / v_total_confirmados::DECIMAL) * 100.0;
        ELSE
            v_perc_saude := 0.0;
        END IF;

        -- Média Vacinas
        SELECT AVG(subquery.qtd_doses)
        INTO v_media_vacina
        FROM (
            SELECT COUNT(v.vacina_id) as qtd_doses
            FROM notificacao n
            LEFT JOIN vacina_aplicada v ON n.notificacao_id = v.notificacao_id
            WHERE n.municipio_notificacao_ibge = reg_municipio.id
              AND n.data_notificacao BETWEEN p_inicio AND p_fim
            GROUP BY n.notificacao_id
        ) subquery;

        -- Atualização (UPSERT)
        INSERT INTO indicadores_regionais (
            municipio_ibge, periodo_inicio, periodo_fim, 
            taxa_positividade, tempo_medio_sintomas_teste, perc_prof_saude_infectados, media_doses_vacina
        )
        VALUES (
            reg_municipio.id, p_inicio, p_fim,
            COALESCE(v_taxa_pos, 0), 
            COALESCE(v_tempo_medio, 0), 
            COALESCE(v_perc_saude, 0), 
            COALESCE(v_media_vacina, 0)
        )
        ON CONFLICT (municipio_ibge, periodo_inicio, periodo_fim) 
        DO UPDATE SET
            taxa_positividade = EXCLUDED.taxa_positividade,
            tempo_medio_sintomas_teste = EXCLUDED.tempo_medio_sintomas_teste,
            perc_prof_saude_infectados = EXCLUDED.perc_prof_saude_infectados,
            media_doses_vacina = EXCLUDED.media_doses_vacina,
            data_processamento = CURRENT_TIMESTAMP;
            
    END LOOP;
END;
$$ LANGUAGE plpgsql;