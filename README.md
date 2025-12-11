# 🏥 Pipeline de Dados e Analytics: e-SUS Notifica (Síndrome Gripal)

Este projeto consiste em uma solução completa de **Engenharia e Análise de Dados** para monitoramento de casos de Síndrome Gripal (incluindo COVID-19). O pipeline abrange desde a ingestão de dados brutos, normalização em banco relacional (PostgreSQL), até a criação de mecanismos de auditoria, automação de indicadores e views analíticas para Dashboards.

---

## 🚀 Funcionalidades do Projeto

### 1. Engenharia de Dados (ETL)
* **Ingestão:** Script Python (`insercao.py`) para carga de dados brutos (CSV) no PostgreSQL.
* **Normalização:** Modelagem de dados relacional separando entidades (Município, Paciente, Exames, Vacinas).
* **Segurança:** Tratamento de duplicatas e uso de variáveis de ambiente (`.env`) para proteção de credenciais.

### 2. Automação e Banco de Dados (SQL Avançado)
* **Auditoria de Dados:** Implementação de `TRIGGERS` que registram qualquer alteração (INSERT/UPDATE/DELETE) na tabela `log_alteracoes`, garantindo rastreabilidade total.
* **Cálculo Automatizado:** Função Armazenada (`fx_calcular_taxa_positividade`) que processa indicadores complexos (Taxa de Positividade, Tempo Médio Sintoma-Teste, Cobertura Vacinal) e armazena em tabela histórica (`indicadores_regionais`).
* **Otimização:** Uso de índices e `CTEs` (Common Table Expressions) para consultas de alta performance.

### 3. Analytics e Business Intelligence
* **Views Analíticas:** Criação de tabelas virtuais otimizadas para consumo direto em ferramentas de BI (PowerBI, Streamlit):
    * `vw_casos_por_municipio`: Evolução temporal e geográfica dos casos e óbitos.
    * `vw_vacinacao_por_resultado`: Correlação entre esquema vacinal e gravidade do caso.
    * `vw_sintomas_frequentes`: Análise de sintomas predominantes em casos confirmados.

---

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python 3.12+
* **Banco de Dados:** PostgreSQL 14+
* **Bibliotecas Python:**
    * `pandas` (Manipulação e Limpeza de DataFrames)
    * `sqlalchemy` & `psycopg2` (Conexão e ORM)
    * `python-dotenv` (Gerenciamento seguro de credenciais)
* **Ferramentas:** VS Code, DBeaver/PgAdmin, Git.

---

## ⚙️ Configuração e Execução

### Pré-requisitos
Certifique-se de ter o Python e o PostgreSQL instalados e configurados.

### 1. Clonar o Repositório
```bash
git clone [https://github.com/seu-usuario/projeto-esus-pipeline.git](https://github.com/seu-usuario/projeto-esus-pipeline.git)
cd projeto-esus-pipeline
```

### 2. Configurar Variáveis de Ambiente
Crie um arquivo chamado .env na raiz do projeto e configure suas credenciais do banco (este arquivo é ignorado pelo Git para segurança):
```bash
DB_USER=postgres
DB_PASS=sua_senha_aqui
DB_HOST=localhost
DB_NAME=Desafio_SUS
```
### 3. Instalar Dependências
```bash
pip install pandas sqlalchemy psycopg2-binary python-dotenv
```
### 4. Preparar o Banco de Dados
Execute o script SQL com as definições de tabelas (Schema) no seu gerenciador de banco de dados.

### 5. Executar o Pipeline
Carga de Dados (ETL):
```bash
python insercao.py
```
Extração para Dashboard:
```bash
python extracao_dashboard.py
```
---
## 📊 Estrutura do Banco de Dados
O banco foi modelado para garantir integridade e performance analítica:

**Tabelas Dimensionais**: estado, municipio, sintoma, condicao.

**Tabelas Fato**: notificacao, dados_clinicos, teste_laboratorial, vacina_aplicada, dados_demograficos.

**Tabelas de Controle**: * log_alteracoes (Auditoria via Trigger).

**indicadores_regionais** (KPIs pré-calculados via Stored Function).

---

## 📈 Exemplo de Uso (SQL)
Para atualizar manualmente os indicadores estatísticos de um período específico no banco de dados:
```sql
-- Calcula indicadores de Janeiro de 2024 (Taxa de Positividade, Médias, etc.)
SELECT fx_calcular_taxa_positividade('2024-01-01', '2024-01-31');
```
```sql
-- Consulta o resultado processado na tabela de indicadores
SELECT * FROM indicadores_regionais;
```

## 📝 Autores
Desenvolvido por José Joaquim Valdez, Lucas Mesquita, Jorge Lobato e Victor de Pinho como parte do Desafio de Banco de Dados.