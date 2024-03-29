
WITH ass_aux AS (
SELECT 
    ass.*,
    clientes.nm_nome_cliente 
FROM read_parquet('s3://silver/postgres_assinaturas/*/*.parquet') as ass
LEFT JOIN read_parquet('s3://silver/postgres_clientes/*/*.parquet') as clientes 
on clientes.id_cliente = ass.id_cliente
)
, cte_aux2 as ( 
SELECT 
        ass_aux.*,
        row_number() OVER (PARTITION BY ass_aux.id_assinatura ORDER BY ass_aux.ts_data_evento ) as indice,
        (row_number() OVER (PARTITION BY ass_aux.id_assinatura ORDER BY ass_aux.ts_data_evento )) + 1  as indice_mais_1,
FROM 
(
    SELECT 
            id_assinatura, 
            vl_valor_mensalidade,
            MIN(ts_data_evento)  as ts_data_evento
    FROM ass_aux
    GROUP BY 1,2
) aux1
LEFT JOIN ass_aux on (ass_aux.id_assinatura = aux1.id_assinatura and ass_aux.ts_data_evento = aux1.ts_data_evento)
) 
, final as (
SELECT 
        cte_aux2.*,
        l_sup.vl_valor_mensalidade as vl_valor_mensalidade_sup,
        l_sup.ts_data_evento as ts_data_evento_sup,
        (l_sup.vl_valor_mensalidade - cte_aux2.vl_valor_mensalidade) as valor_expansao
FROM cte_aux2
LEFT JOIN cte_aux2 as l_sup on (l_sup.id_assinatura = cte_aux2.id_assinatura and cte_aux2.indice_mais_1 = l_sup.indice)
WHERE ( l_sup.vl_valor_mensalidade is not null and cte_aux2.vl_valor_mensalidade is not null )
ORDER BY cte_aux2.vl_valor_mensalidade ASC, cte_aux2.ts_data_evento DESC
)
SELECT 
    id_assinatura, 
    id_cliente, 
    nm_nome_cliente,
    nm_possui_plano_pro,
    nm_template_log,
    vl_valor_mensalidade as valor_anterior,
    vl_valor_mensalidade_sup as valor_novo,
    ts_data_evento_sup,
    valor_expansao,
    data_partition    
FROM final