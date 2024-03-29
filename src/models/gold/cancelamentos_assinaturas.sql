WITH aux AS (
SELECT 
    ass.*,
    clientes.nm_nome_cliente 
FROM read_parquet('s3://silver/postgres_assinaturas/*/*.parquet') as ass
LEFT JOIN read_parquet('s3://silver/postgres_clientes/*/*.parquet') as clientes 
on clientes.id_cliente = ass.id_cliente
)
, final as (
SELECT 
        aux.id_assinatura, 
        aux.id_cliente,
        aux.nm_nome_cliente,  
        aux.vl_valor_mensalidade, 
        aux.nm_possui_plano_pro, 
        aux.ts_data_evento,
        aux.data_partition
FROM aux
where nm_template_log = 'template_delete_preenchido'
ORDER BY aux.id_assinatura ASC
) 
SELECT * FROM final