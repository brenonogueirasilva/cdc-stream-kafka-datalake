import json
import requests
import time 
import logging

from class_and_functions.conect_minio import ConectMinio
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

time.sleep(30)

log_file_name = f"/code/logs/process_raw_to_bronze/process_raw_to_bronze{datetime.today().date()}.log"
logging.basicConfig(
    level=logging.DEBUG, 
    format="%(asctime)s :: %(levelname)s :: %(filename)s :: :: %(lineno)d :: %(message)s",
    handlers=[
        logging.FileHandler(log_file_name),
        logging.StreamHandler()
    ]
                    )

creds_minio_path = '/code/config/creds_minio.json'
with open(creds_minio_path, 'r') as creds_file:
    creds_minio = json.loads(creds_file.read())
host = creds_minio['host'] 
port = creds_minio['porta'] 
access_key = creds_minio['access_key'] 
secret_key = creds_minio['secret_key'] 

ls_buckets = ['raw', 'bronze', 'silver', 'gold']

minio_connector = ConectMinio(host, port, access_key, secret_key)

logging.info('Criando Buckets que serão utilizando no MINIO')
for bucket in ls_buckets:
    minio_connector.create_bucket(bucket)
#print('\n')


#Criando Topicos raw, bronze, silver, gold

bootstrap_servers = 'kafka:9092' 
ls_topics = ['raw', 'bronze', 'silver', 'gold']
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

for topic in ls_topics:
    new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
    try:
        admin_client.create_topics([new_topic])
        logging.info(f"Tópico '{topic}' criado com sucesso!")
    except TopicAlreadyExistsError:
        logging.warning(f"Tópico '{topic}' já existe.")


# Criando Topico CDC e ativando o conector debezium

debezium_config_path = '/code/config/debezium_config.json'
url = 'http://cdc-using-debezium-connect:8083/connectors'
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

with open(debezium_config_path, 'r') as config_file:
    debezium_config = json.loads(config_file.read())


payload = json.dumps(debezium_config)
response = requests.post(
    url=url, 
    headers=headers,
    data = payload
    )
logging.info('\nStatus Code:', response.status_code)
logging.info('Texto de Retorno:', response.text)
#print('\n')