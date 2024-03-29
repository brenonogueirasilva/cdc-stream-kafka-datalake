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

time.sleep(40)

log_file_name = f"/code/logs/process_raw_to_bronze/process_raw_to_bronze{datetime.today().date()}.log"
logging.basicConfig(
    level=logging.DEBUG, 
    format="%(asctime)s :: %(levelname)s :: %(filename)s :: :: %(lineno)d :: %(message)s",
    handlers=[
        logging.FileHandler(log_file_name),
        logging.StreamHandler()
    ]
                    )

bootstrap_servers = 'kafka:9092'
topico_origem = 'raw'
topico_destino = 'bronze'
bucket_origem = 'raw'
bucket_destino = 'bronze'

template_insert_update_path = '/code/models/bronze/template_insert_update.sql'
template_delete_preenchido_path = '/code/models/bronze/template_delete_preenchido.sql'
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

logging.info('Consumindo Topico Raw do Kafka ...')
for message in consumer:
    mensagem_value = json.loads(message.value)
    
    template = mensagem_value['template']
    data_envio = datetime.strptime(mensagem_value['data_envio'], '%Y-%m-%d %H:%M:%S')
    data_envio_particao = data_envio.date()
    data_evento = str(data_envio)
    pasta_minio = mensagem_value['pasta_minio']
    nome_arquivo = mensagem_value['nome_arquivo']
    caminho_arquivo = f"'s3://{bucket_origem}/{nome_arquivo}'"

    def gerando_sql_arquivo_json():    #template, caminho_arquivo, data_evento):
        sql_template_insert_update =  func_aux.treat_sql_model(template_insert_update_path, globals())
        sql_template_delete_preenchido =  func_aux.treat_sql_model(template_delete_preenchido_path, globals())
        if template == 'template_insert_update':
            sql_raw_1 = sql_template_insert_update
        elif template == 'template_delete_preenchido':
            sql_raw_1 = sql_template_delete_preenchido
        elif template == 'template_delete_vazio':
            return None 
        return sql_raw_1

    sql_arquivo_json = gerando_sql_arquivo_json()

    check_pasta_destino_existe = minio_connector.check_folder_in_bucket(bucket_destino, pasta_minio)

    if sql_arquivo_json is not None:
        sql_insert_lake = func_aux.treat_query_lake(minio_sql_minio, check_pasta_destino_existe, sql_arquivo_json, bucket_destino, pasta_minio, data_envio_particao   )
        minio_sql_minio.executing_query(sql_insert_lake)

    mensagem_topico_destino = {
        "topico_remetente" : topico_origem,
        "pasta_minio" : pasta_minio,
        "data_envio" : str(data_envio)
    }
    logging.info(f'Arquivo {nome_arquivo} foi processado com sucesso, bucket {bucket_destino} já contém o arquivo na pasta {pasta_minio}, caminho completo {caminho_arquivo} ')
    producer.send(topico_destino, value= mensagem_topico_destino)
    #print('\n')
