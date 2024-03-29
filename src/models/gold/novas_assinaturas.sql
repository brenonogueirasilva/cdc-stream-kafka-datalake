
WITH aux AS (
SELECT 
    ass.*,
    clientes.nm_nome_cliente 
FROM read_parquet('s3://silver/postgres_assinaturas/*/*.parquet') as ass
LEFT JOIN read_parquet('s3://silver/postgres_clientes/*/*.parquet') as clientes 
on clientes.id_cliente = ass.id_cliente
)
, aux1 as (
SELECT 
        aux.id_assinatura, 
        aux.id_cliente,
        aux.nm_nome_cliente,  
        aux.vl_valor_mensalidade, 
        aux.nm_possui_plano_pro, 
        aux.ts_data_evento,
        aux.data_partition
FROM
(
    SELECT 
            id_assinatura, 
            MIN(ts_data_evento) as ts_data_evento
    from aux
    GROUP BY 1
) aux1
LEFT JOIN aux on (aux.id_assinatura = aux1.id_assinatura and aux.ts_data_evento = aux1.ts_data_evento ) 
ORDER BY aux.id_assinatura ASC
)
SELECT * FROM aux1