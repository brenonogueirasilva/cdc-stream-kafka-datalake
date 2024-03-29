SELECT 
    key.payload.id as id_payload,
    unnest(value.payload.before),
    '{&template&}' as template_log,
    CAST('{&data_evento&}' as timestamp) as update_date,
    CAST('{&data_evento&}' as date) as data_partition
FROM read_json_auto({&caminho_arquivo&})