-- insights.sql

-- =====================================================================================
-- COMPARAÇÃO DE CONSULTAS: Camada GOLD (Star Schema) vs. Camada SILVER (Normalizada)
-- Objetivo: Demonstrar a simplicidade de consulta e a otimização de desempenho
--           proporcionada pelo Star Schema na camada Gold.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- INSIGHT 1: Faturamento Total por Tipo de Encontro e Ano
-- -------------------------------------------------------------------------------------
-- Descrição: Qual o custo total de atendimentos por tipo de encontro e por ano?
--            Esta é uma consulta analítica comum.

-- CONSULTA NA CAMADA GOLD (STAR SCHEMA)
-- Benefícios esperados: Menos JOINS, atributos prontos nas dimensões.
EXPLAIN ANALYSE
SELECT
    det.encounter_type AS tipo_encontro,
    dd.year AS ano_encontro,
    SUM(fe.total_claim_cost) AS custo_total_encontro
FROM gold_fact_encounters fe
JOIN gold_dim_encounter_type det ON fe.encounter_type_sk = det.encounter_type_sk
JOIN gold_dim_date dd ON fe.encounter_date_sk = dd.date_sk
GROUP BY det.encounter_type, dd.year
ORDER BY det.encounter_type, dd.year;

-- CONSULTA NA CAMADA SILVER (NORMALIZADA)
-- Desvantagens esperadas: Mais JOINS, talvez funções de data para extrair o ano.
EXPLAIN ANALYSE
SELECT
    se.encounter_type AS tipo_encontro,
    EXTRACT(YEAR FROM se.encounter_date) AS ano_encontro,
    SUM(se.total_claim_cost) AS custo_total_encontro
FROM silver_fact_encounter se
GROUP BY se.encounter_type, EXTRACT(YEAR FROM se.encounter_date)
ORDER BY se.encounter_type, ano_encontro;

-- -------------------------------------------------------------------------------------
-- INSIGHT 2: Top 5 Pacientes com Maior Custo Total de Claims em um Ano Específico
-- -------------------------------------------------------------------------------------
-- Descrição: Identificar os pacientes que geraram os maiores custos totais de claims
--            em um ano específico (ex: 2023).

-- CONSULTA NA CAMADA GOLD (STAR SCHEMA)
-- Benefícios esperados: Acesso direto a nomes e anos pré-calculados.
EXPLAIN ANALYSE
SELECT
    dp.full_name AS nome_paciente,
    dd.year AS ano_claim,
    SUM(fc.total_outstanding) AS custo_total_claims
FROM gold_fact_claims fc
JOIN gold_dim_patient dp ON fc.patient_sk = dp.patient_sk
JOIN gold_dim_date dd ON fc.claim_start_date_sk = dd.date_sk
WHERE dd.year = 2023 -- Exemplo para um ano específico
GROUP BY dp.full_name, dd.year
ORDER BY custo_total_claims DESC
LIMIT 5;

-- CONSULTA NA CAMADA SILVER (NORMALIZADA)
-- Desvantagens esperadas: JOINS com tabelas de pacientes, extração do ano da data.
EXPLAIN ANALYSE
SELECT
    sp.full_name AS nome_paciente,
    EXTRACT(YEAR FROM sfc.claim_start_date) AS ano_claim,
    SUM(sfc.total_outstanding) AS custo_total_claims
FROM silver_fact_claim sfc
JOIN silver_dim_patient sp ON sfc.patient_sk = sp.patient_sk
WHERE EXTRACT(YEAR FROM sfc.claim_start_date) = 2023 -- Exemplo para um ano específico
GROUP BY sp.full_name, EXTRACT(YEAR FROM sfc.claim_start_date)
ORDER BY custo_total_claims DESC
LIMIT 5;

-- -------------------------------------------------------------------------------------
-- INSIGHT 3: Média de Permanência (Length of Stay) por Tipo de Encontro e Mês/Ano
-- -------------------------------------------------------------------------------------
-- Descrição: Qual a média de dias de permanência em encontros, agrupado por tipo e
--            período (mês/ano)?

-- CONSULTA NA CAMADA GOLD (STAR SCHEMA)
-- Benefícios esperados: Atributos de tempo (mês, ano) e length_of_stay_days já disponíveis.
EXPLAIN ANALYSE
SELECT
    det.encounter_type AS tipo_encontro,
    dd.year AS ano,
    dd.month_name AS mes,
    AVG(fe.length_of_stay_days) AS media_permanencia_dias
FROM gold_fact_encounters fe
JOIN gold_dim_encounter_type det ON fe.encounter_type_sk = det.encounter_type_sk
JOIN gold_dim_date dd ON fe.encounter_date_sk = dd.date_sk
WHERE fe.length_of_stay_days IS NOT NULL AND fe.length_of_stay_days >= 0
GROUP BY det.encounter_type, dd.year, dd.month_name
ORDER BY det.encounter_type, dd.year, dd.month_name;

-- CONSULTA NA CAMADA SILVER (NORMALIZADA)
-- Desvantagens esperadas: Cálculo de `length_of_stay_days`, extração de mês/ano, JOINS.
EXPLAIN ANALYSE
SELECT
    sfe.encounter_type AS tipo_encontro,
    EXTRACT(YEAR FROM sfe.encounter_date) AS ano,
    EXTRACT(MONTH FROM sfe.encounter_date) AS mes_num, -- O nome do mês exigiria um CASE ou JOIN adicional
    AVG(sfe.length_of_stay_days) AS media_permanencia_dias
FROM silver_fact_encounter sfe
WHERE sfe.length_of_stay_days IS NOT NULL AND sfe.length_of_stay_days >= 0
GROUP BY sfe.encounter_type, EXTRACT(YEAR FROM sfe.encounter_date), EXTRACT(MONTH FROM sfe.encounter_date)
ORDER BY sfe.encounter_type, ano, mes_num;

-- -------------------------------------------------------------------------------------
-- INSIGHT 4: Distribuição de Claims por Faixa Etária dos Pacientes
-- -------------------------------------------------------------------------------------
-- Descrição: Quantas claims foram abertas para cada faixa etária de pacientes?

-- CONSULTA NA CAMADA GOLD (STAR SCHEMA)
-- Benefícios esperados: Faixa etária pronta na dimensão do paciente.
EXPLAIN ANALYSE
SELECT
    dp.age_group AS faixa_etaria,
    COUNT(fc.claim_id) AS total_claims
FROM gold_fact_claims fc
JOIN gold_dim_patient dp ON fc.patient_sk = dp.patient_sk
GROUP BY dp.age_group
ORDER BY dp.age_group;

-- CONSULTA NA CAMADA SILVER (NORMALIZADA)
-- Desvantagens esperadas: Recalcular a faixa etária em cada consulta, JOINS.
EXPLAIN ANALYSE
SELECT
    CASE
        WHEN (CURRENT_DATE - sp.date_of_birth) / 365.25 <= 12 THEN '0-12'
        WHEN (CURRENT_DATE - sp.date_of_birth) / 365.25 <= 17 THEN '13-17'
        WHEN (CURRENT_DATE - sp.date_of_birth) / 365.25 <= 25 THEN '18-25'
        WHEN (CURRENT_DATE - sp.date_of_birth) / 365.25 <= 35 THEN '26-35'
        WHEN (CURRENT_DATE - sp.date_of_birth) / 365.25 <= 45 THEN '36-45'
        WHEN (CURRENT_DATE - sp.date_of_birth) / 365.25 <= 60 THEN '46-60'
        WHEN (CURRENT_DATE - sp.date_of_birth) / 365.25 <= 75 THEN '61-75'
        ELSE '75+'
    END AS faixa_etaria,
    COUNT(sfc.claim_id) AS total_claims
FROM silver_fact_claim sfc
JOIN silver_dim_patient sp ON sfc.patient_sk = sp.patient_sk
GROUP BY faixa_etaria
ORDER BY faixa_etaria;