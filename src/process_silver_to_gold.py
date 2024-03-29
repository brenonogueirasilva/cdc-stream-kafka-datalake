import duckdb
from datetime import datetime 
from kafka import KafkaConsumer, KafkaProducer
import json
import time 
import logging

from class_and_functions.conect_minio import ConectMinio
from class_and_functions.sql_minio import SQLMinio
from class_and_functions import func_aux

log_file_name = f"/code/logs/process_silver_to_gold/process_silver_to_gold{datetime.today().date()}.log"
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
topico_origem = 'silver'
topico_destino = 'gold'
bucket_origem = 'silver'
bucket_destino = 'gold' 

creds_minio_path = '/code/config/creds_minio.json'
with open(creds_minio_path, 'r') as creds_file:
    creds_minio = json.loads(creds_file.read())

host = creds_minio['host'] 
porta = creds_minio['porta'] 
access_key = creds_minio['access_key'] 
secret_key = creds_minio['secret_key']

minio_connector = ConectMinio(host, porta, access_key, secret_key) 
minio_sql_minio = SQLMinio(host, porta, access_key, secret_key)

alteracoes_assinaturas_path = '/code/models/gold/alteracoes_assinaturas.sql'
alteracoes_assinaturas_tabela = 'alteracoes_assinaturas'
cancelamentos_assinaturas_path = '/code/models/gold/cancelamentos_assinaturas.sql'
cancelamentos_assinaturas_tabela = 'cancelamentos_assinaturas'
novas_assinaturas_path = '/code/models/gold/novas_assinaturas.sql'
novas_assinaturas_tabela = 'novas_assinaturas'


mensagem_value = {
	"topico_remetente": "bronze",
	"pasta_minio": "postgres_assinaturas",
	"data_envio": "2024-03-27 15:57:38"
}


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


logging.info('Consumindo Topico Silver do Kafka ...')
for message in consumer:
    mensagem_value = json.loads(message.value)

    data_envio = datetime.strptime(mensagem_value['data_envio'], '%Y-%m-%d %H:%M:%S')
    data_envio_particao = data_envio.date()
    pasta_minio = mensagem_value['pasta_minio']

    data_envio = datetime.strptime(mensagem_value['data_envio'], '%Y-%m-%d %H:%M:%S')
    data_envio_particao = data_envio.date()

    def execuntado_tabela_consulta(tabela, path):
        check_pasta = minio_connector.check_folder_in_bucket(bucket_destino, tabela)
        consulta = func_aux.treat_sql_model(path, globals())
        sql_insert_lake = func_aux.treat_query_lake(minio_sql_minio, check_pasta, consulta, bucket_destino, tabela, data_envio_particao   )
        minio_sql_minio.executing_query(sql_insert_lake)

    #Tabela Alteracoes assinaturas
    execuntado_tabela_consulta(alteracoes_assinaturas_tabela, alteracoes_assinaturas_path)

    #Tabela Cancelamentos
    execuntado_tabela_consulta(cancelamentos_assinaturas_tabela, cancelamentos_assinaturas_path)

    #Tabela Novas Asssinaturas
    execuntado_tabela_consulta(novas_assinaturas_tabela, novas_assinaturas_path)

    mensagem_topico_destino = {
        "topico_remetente" : topico_origem,
        "pasta_minio" : pasta_minio,
        "data_envio" : str(data_envio)
    }
    logging.info(f'Bucket {bucket_destino} foi processado com sucesso, data_envio {data_envio}')
    producer.send(topico_destino, value= mensagem_topico_destino)
    #print('\n')

