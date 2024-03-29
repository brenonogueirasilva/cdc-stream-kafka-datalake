import duckdb
import os
import re
from datetime import datetime 
from kafka import KafkaConsumer, KafkaProducer
import json
import time
import logging

from class_and_functions.conect_minio import ConectMinio
from class_and_functions.sql_minio import SQLMinio
from class_and_functions import func_aux

log_file_name = f"/code/logs/process_bronze_to_silver/process_bronze_to_silver{datetime.today().date()}.log"
logging.basicConfig(
    level=logging.DEBUG, 
    format="%(asctime)s :: %(levelname)s :: %(filename)s :: :: %(lineno)d :: %(message)s",
    handlers=[
        logging.FileHandler(log_file_name),
        logging.StreamHandler()
    ]
                    )

time.sleep(40)

bootstrap_servers = 'kafka:9092'
topico_origem = 'bronze'
topico_destino = 'silver'
bucket_origem = 'bronze'
bucket_destino = 'silver'

creds_minio_path = '/code/config/creds_minio.json'
with open(creds_minio_path, 'r') as creds_file:
    creds_minio = json.loads(creds_file.read())

host = creds_minio['host'] 
porta = creds_minio['porta'] 
access_key = creds_minio['access_key'] 
secret_key = creds_minio['secret_key']

minio_connector = ConectMinio(host, porta, access_key, secret_key) 
minio_sql_minio = SQLMinio(host, porta, access_key, secret_key)


#Consumindo Topico Kafka
consumer = KafkaConsumer(
    topico_origem,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',  # Começar do offset mais recente
    enable_auto_commit=True,     # Habilitar auto commit dos offsets
    group_id='my_group_id',
    value_deserializer=lambda x : x.decode('utf-8')
                        )

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

logging.info('Consumindo Topico Bronze do Kafka ...')
for message in consumer:
    mensagem_value = json.loads(message.value)

    data_envio = datetime.strptime(mensagem_value['data_envio'], '%Y-%m-%d %H:%M:%S')
    data_envio_particao = data_envio.date()
    pasta_minio = mensagem_value['pasta_minio']

    sql_lake = minio_sql_minio.generate_sql_select_minio_lake(bucket_origem, pasta_minio, data_envio_particao)

    trataticas_file_path = f"/code/models/silver/{pasta_minio}.json"
    with open(trataticas_file_path, 'r') as trataticas_file:
        tratatica_json = json.loads(trataticas_file.read())

    ls_colunas = []
    for chave, valor in tratatica_json.items():
        linha = f'"{chave}" as {valor}'
        ls_colunas.append(linha)
    consulta_colunas = ", \n".join(ls_colunas)
    consulta_colunas = f'''
    SELECT 
    {consulta_colunas}'''
    sql_lake_tratada = sql_lake.replace('SELECT *', consulta_colunas)
   
    check_pasta_destino_existe = minio_connector.check_folder_in_bucket(bucket_destino, pasta_minio)


    sql_insert_lake = func_aux.treat_query_lake(minio_sql_minio, check_pasta_destino_existe, sql_lake_tratada, bucket_destino, pasta_minio, data_envio_particao   )
    minio_sql_minio.executing_query(sql_insert_lake)
    
    mensagem_topico_destino = {
        "topico_remetente" : topico_origem,
        "pasta_minio" : pasta_minio,
        "data_envio" : str(data_envio)
    }
    logging.info(f'Bucket {bucket_destino} foi processado com sucesso, data_envio {data_envio}')
    producer.send(topico_destino, value= mensagem_topico_destino)
    #print('\n')



