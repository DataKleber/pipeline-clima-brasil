-- 1. Temperatura atual de todas as cidades
SELECT cidade, temperatura_c, descricao
FROM clima_cidades
ORDER BY temperatura_c DESC;

-- 2. Cidade mais quente registrada
SELECT cidade, MAX(temperatura_c) as max_temperatura
FROM clima_cidades
GROUP BY cidade
ORDER BY max_temperatura DESC
LIMIT 1;

-- 3. Media de temperatura por cidade
SELECT cidade, ROUND(AVG(temperatura_c)::numeric, 1) as media_temp
FROM clima_cidades
GROUP BY cidade
ORDER BY media_temp DESC;

-- 4. Cidades com umidade acima de 80%
SELECT cidade, umidade_pct, temperatura_c
FROM clima_cidades
WHERE umidade_pct > 80
ORDER BY umidade_pct DESC;

-- 5. Ultima coleta de cada cidade
SELECT DISTINCT ON (cidade) cidade, temperatura_c, descricao, processado_em
FROM clima_cidades
ORDER BY cidade, processado_em DESC;

-- Qual foi a 1a, 2a, 3a coleta de cada cidade?
SELECT
    cidade,
    temperatura_c,
    extraido_em,
    ROW_NUMBER() OVER (PARTITION BY cidade ORDER BY extraido_em) AS numero_coleta
FROM clima_cidades
ORDER BY cidade, numero_coleta;

-- Qual foi a 1a, 2a, 3a coleta de cada cidade?
SELECT
    cidade,
    temperatura_c,
    extraido_em,
    ROW_NUMBER() OVER (PARTITION BY cidade ORDER BY extraido_em) AS numero_coleta
FROM clima_cidades
ORDER BY cidade, numero_coleta;