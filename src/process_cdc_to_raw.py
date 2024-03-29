import json 
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import time 
import random
import yaml
import logging
from datetime import datetime 

from class_and_functions.conect_minio import ConectMinio

log_file_name = f"/code/logs/process_cdc_to_raw/process_cdc_to_raw{datetime.today().date()}.log"
logging.basicConfig(
    level=logging.DEBUG, 
    format="%(asctime)s :: %(levelname)s :: %(filename)s :: :: %(lineno)d :: %(message)s",
    handlers=[
        logging.FileHandler(log_file_name),
        logging.StreamHandler()
    ]
                    )


def generate_file_id():
    timestamp = int(time.time())
    numero_aleatorio = random.randint(1, 100000)
    id_mensagem = int(str(timestamp) + str(numero_aleatorio))
    return id_mensagem

time.sleep(40)

creds_minio_path = '/code/config/creds_minio.json'
with open(creds_minio_path, 'r') as creds_file:
    creds_minio = json.loads(creds_file.read())

host = creds_minio['host'] 
porta = creds_minio['porta'] 
access_key = creds_minio['access_key'] 
secret_key = creds_minio['secret_key']

minio_connector = ConectMinio(host, porta, access_key, secret_key)

bootstrap_servers = 'kafka:9092'
caminho_arquivo_yaml = '/code/config/topicos_cdc.yaml'
with open(caminho_arquivo_yaml, 'r') as arquivo:
    dados_yaml = yaml.safe_load(arquivo)

ls_topics = list(dados_yaml.keys())

nome_bucket = 'raw'
nome_topico = 'raw'

def custom_deserializer(value):
    if value is not None:
        return value.decode('utf-8')
    else:
        return None
    
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',  # Começar do offset mais recente
    enable_auto_commit=True,     # Habilitar auto commit dos offsets
    group_id='my_group_id',
    value_deserializer=custom_deserializer
)
consumer.subscribe(ls_topics)

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

logging.info('Consumindo Topicos Kafka Tabelas CDC ...')

for message in consumer:
    data_atual = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    topico = message.topic
    pasta_minio = dados_yaml[topico]['pasta_minio']
    key = json.loads(message.key)

    try:
        value = json.loads(message.value)
        possui_after = value['payload']['after']
        if possui_after is not None: 
            template_json = 'template_insert_update'
            nome_arquivo = f"{pasta_minio}/{template_json}_id({generate_file_id()}).json"
        else:
            template_json = 'template_delete_preenchido'
            nome_arquivo = f"{pasta_minio}/{template_json}_id({generate_file_id()}).json"
    except:
        template_json = 'template_delete_vazio'
        value = None
        nome_arquivo = f"{pasta_minio}/{template_json}_id({generate_file_id()}).json" 

    dicionario_minio = {}
    dicionario_minio['key'] = key
    dicionario_minio['value'] = value

    minio_connector.insert_string_to_minio(nome_bucket, dicionario_minio, nome_arquivo)
    logging.info(f'Arquivo {nome_arquivo}, do topico {topico} foi inserido no Minio com sucesso!')

    mensagem_topico_raw = {
        "topico" : topico,
        "template" : template_json,
        "pasta_minio" : pasta_minio,
        "data_envio" :  data_atual,
        "nome_arquivo" : nome_arquivo
    }
    producer.send(nome_topico, value= mensagem_topico_raw )
    #print('\n')