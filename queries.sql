SELECT * FROM "default"."ibov_cleaned" limit 10;

--

SELECT
    codigo_acao,
    data_fechamento,
    fechamento,
    variacao_diaria
FROM "default"."ibov_cleaned"
WHERE
    codigo_acao = 'ALOS3'   
    AND ano = '2025'          
    AND mes = '10'            
    AND dia = '14'            
LIMIT 10;

