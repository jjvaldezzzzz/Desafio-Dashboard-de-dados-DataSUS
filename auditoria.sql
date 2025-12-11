-- 1. Tabela de Log Unificada
CREATE TABLE IF NOT EXISTS log_alteracoes (
    log_id SERIAL PRIMARY KEY,
    tabela_afetada VARCHAR(50) NOT NULL,
    operacao CHAR(1) NOT NULL, -- 'I' (Insert), 'U' (Update), 'D' (Delete)
    usuario_db VARCHAR(50) DEFAULT current_user,
    data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dados_antigos JSONB, -- O que havia antes (para Update/Delete)
    dados_novos JSONB    -- O que foi gravado (para Insert/Update)
);

-- 2. Função Gatilho (Trigger Function) Genérica
CREATE OR REPLACE FUNCTION fx_auditoria_geral()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO log_alteracoes (tabela_afetada, operacao, dados_novos)
        VALUES (TG_TABLE_NAME, 'I', row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_alteracoes (tabela_afetada, operacao, dados_antigos, dados_novos)
        VALUES (TG_TABLE_NAME, 'U', row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO log_alteracoes (tabela_afetada, operacao, dados_antigos)
        VALUES (TG_TABLE_NAME, 'D', row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 3. Aplicando os Triggers nas tabelas críticas
DROP TRIGGER IF EXISTS trg_audit_notificacao ON notificacao;
CREATE TRIGGER trg_audit_notificacao
AFTER INSERT OR UPDATE OR DELETE ON notificacao
FOR EACH ROW EXECUTE FUNCTION fx_auditoria_geral();

DROP TRIGGER IF EXISTS trg_audit_testes ON teste_laboratorial;
CREATE TRIGGER trg_audit_testes
AFTER INSERT OR UPDATE OR DELETE ON teste_laboratorial
FOR EACH ROW EXECUTE FUNCTION fx_auditoria_geral();

-- (Opcional) Trigger em Dados Clínicos também é importante
DROP TRIGGER IF EXISTS trg_audit_clinicos ON dados_clinicos;
CREATE TRIGGER trg_audit_clinicos
AFTER INSERT OR UPDATE OR DELETE ON dados_clinicos
FOR EACH ROW EXECUTE FUNCTION fx_auditoria_geral();