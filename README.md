# Projeto de Engenharia de Dados: Disponibilização de Tabelas em Tempo Real em Ambiente Datalake através de Streaming de Dados com CDC e Apache Kafka 

## Introdução

Em determinados contextos de engenharia de dados, surge a necessidade de disponibilizar dados de uma tabela transacional em tempo real. Para isso, é crucial utilizar streaming de dados, que envolve a coleta, ingestão e processamento de dados em tempo real. Este projeto tem como objetivo desenvolver uma pipeline capaz de coletar dados em tempo real de um banco transacional Postgres, utilizando para isso um conector CDC chamado Debezium, juntamente com o Apache Kafka, uma tecnologia popular de streaming. Os dados serão então disponibilizados em um datalake, usando o Minio como armazenamento de objeto e o DuckDB como engine de processamento. Todo o projeto será desenvolvido em ambiente local com o uso do Docker. 

## Tecnologias Utilizadas

- **Postgres:** Banco de dados relacional que irá consistir como origem dos dados.
- **Docker:** tecnologia que permite executar todas as tecnologias do projeto através de contêineres.  
- **Kafka:** plataforma de streaming de dados distribuída, usada para processamento de eventos em tempo real e armazenamento de fluxos de dados em larga escala. 
- **Debezium:** conector de captura de dados de alteração (CDC) open-source, utilizado para capturar mudanças em bancos de dados e transformá-las em um fluxo de eventos em tempo real. 
- **Python:** Linguagem de programação utilizada para realizar toda a etapa de processamento.
- **DuckDb:** biblioteca multiuso que dentre suas funções é capaz ser utilizada como engine de processamento, sendo capaz de realizar consultas diretamente no Minio.
- 
<p align="left">
<img src="/img/postgres-logo.png" alt="postgres-logo" height="50" /> 
<img src="/img/docker-logo.png" alt="docker" height="50" />
<img src="/img/kafka-logo.png" alt="kafka-logo" height="50" />
<img src="/img/debezium-logo.png" alt="debezium" height="50" />
<img src="/img/python-logo.png" alt="python" height="50" />
<img src="/img/duckdb-logo.png" alt="duckdb" height="50" />  
</p>

## Arquitetura

![Diagrama de Arquitetura](img/arquitetura_cdc_kafka.png)

## Etapas do Projeto

O provisionamento da infraestrutura é conduzido através do Terraform, sendo organizado em três pastas distintas. A primeira pasta, denominada "cloudsql" (link), é utilizada para a configuração inicial do banco de dados, criando a instância necessária. A segunda, chamada "stream" (link), abrange a provisão completa da infraestrutura do projeto, desde a criação do script até a configuração do Datastream. Por fim, a última pasta, chamada "trigger" (link), é responsável por configurar uma trigger no Cloud Build associada a um repositório. Dessa forma, sempre que uma mudança é realizada no script ou na infraestrutura, basta fazer um commit no Git para que o deploy seja automaticamente acionado. 

### 1. Configuração Inicial do Banco de Dados Postgres

Na primeira etapa do projeto, é imprescindível realizar o provisionamento de um banco de dados transacional, sendo o Postgres a escolha feita para desempenhar essa função como fonte de dados primária. Este banco será responsável pela replicação dos dados em tempo real. A coleta dos dados será realizada por meio do CDC (Capture Data Changes) ou captura de dados alterados, que consiste na leitura constante dos logs binários do banco. Isso permite rastrear em tempo real qualquer evento que ocorra nas tabelas, reduzindo o impacto nas transações do banco, uma vez que não é uma consulta direta ao banco, mas sim uma leitura de seus logs. 

Para que o banco gere este logs que serão consultados para ser feito o CDC, é necessário realizar uma configuração específica que ativa os requisitos necessários, para isso todas estas configurações são feitas através do arquivo init.sql {colocar link}, que são comandos que serão executados no container do postgres durante sua inicialização. 

### 2. Configuração Inicial do Apache Kafka e Conector Debezium

Após o banco de dados ser configurado para gerar os logs necessários para ser feito o CDC, é hora de provisionar o serviço responsável por receber esses logs, que é o Apache Kafka. O Kafka é uma tecnologia open source com uma ampla gama de casos de uso. No contexto deste projeto, ele é utilizado como um sistema de mensageria assíncrono com padrão Pub/Sub (Publicação/Assinatura), o que ajuda em situações de eventual indisponibilidade dos serviços de processamento, que serão discutidos posteriormente. Além disso, o Kafka é capaz de encaminhar as mensagens para qualquer consumidor interessado.

Para que o Kafka possa receber os logs gerados pelo banco sempre que ocorrerem eventos nas tabelas configuradas, é necessário utilizar um conector específico capaz de lidar com o CDC. Para essa finalidade, será utilizado o Debezium, que realiza uma leitura constante do Postgres e envia mensagens para o Kafka sempre que ocorrer qualquer tipo de evento nas tabelas.

### 3. Configuração Inicial do Minio como Armazenamento de Objetos

Os dados finais serão disponibilizados por meio da arquitetura de armazenamento DataLake. Para isso, será necessário um serviço de armazenamento de objetos chamado Minio. Este serviço irá possuir quatro buckets principais:
1. RAW: Este bucket será utilizado como armazenamento primário das mensagens dos logs, salvando os dados em seu formato bruto, sem qualquer tipo de processamento adicional.
2. Bronze: Esta camada consiste em ler os arquivos no formato bruto e salvar os mesmos dados no formato parquet. O formato parquet é um tipo de armazenamento colunar altamente otimizado, capaz de reduzir consideravelmente o espaço de armazenamento e aumentar a eficiência de consulta.
3. Silver: Esta camada basicamente irá modificar os nomes das colunas e realizar alguma mudança de tipagem nos dados.
4. Gold: Aqui será armazenada a tabela final que será utilizada para consumo. Esta tabela ficará no formato mais próximo do que o time de negócios ou os analistas de dados necessitam.

### 4. Desenvolvimento do Script de Processamento para Leitura e Escrita dos Dados em Cada uma das Camadas do Data Lake

Com todos os serviços provisionados disponíveis, agora é o momento de configurar o processamento dos logs provenientes do banco e criar as eventuais ETLs para cada uma das camadas. Para isso, serão criados quatro containers principais:
- **Config_init:** Este container realiza as configurações iniciais necessárias para o início da pipeline. Ele consiste em criar os quatro buckets necessários no Minio, criar também no Apache Kafka os tópicos para cada um dos buckets, que serão utilizados como gatilhos para os processamentos, e por fim, realizar a configuração do conector Debezium com as tabelas que devem ser lidas no banco, o que é feito através de uma requisição POST no container do Debezium.
- **Process_cdc_to_raw:** Este container, ao ser inicializado, realiza uma leitura contínua dos tópicos das tabelas do Kafka. Cada tabela que sofrerá CDC terá seu próprio tópico no Kafka. Assim que uma nova mensagem contendo um log informando algum evento da tabela é recebida no tópico, o container lê essa mensagem e escreve seu conteúdo como JSON na camada RAW no Minio. Além disso, envia uma mensagem para o tópico RAW informando a escrita de um novo objeto, contendo informações de datas, nome do tópico e tabela a qual o JSON se refere.
- **Process_raw_to_bronze:** Este container lê continuamente o tópico RAW e, assim que uma nova mensagem é recebida, informando o caminho de um novo arquivo JSON na RAW, realiza um SELECT no RAW e escreve os mesmos dados formatados como parquet na camada Bronze.
- **Process_bronze_to_silver:** Este container lê o tópico Bronze e, assim que aparece uma nova notificação, consulta os dados da Bronze e realiza uma transformação em relação aos nomes das colunas. Em seguida, escreve na camada Silver e envia uma mensagem informando o tópico Silver.
- **Process_silver_to_gold:** Este container lê o tópico Silver e, assim com novas mensagens, transforma a Silver e formata as tabelas na estrutura solicitada pela área de negócios ou análise de dados. Em seguida, escreve o formato na camada Gold, local no qual os dados serão consumidos.

## Observação
Todo o processamento dos dados é feito através de consultas SQL dentro do próprio Minio, que são salvos fora dos scripts dos containers, e sim na pasta models {colocar link}, simplificando assim o processo de manutenção e até de novas tabelas que serão incluídas. (*Inspiração a partir do DBT, que não é capaz de transformar dados dentro de um datalake, justificando o seu não uso para as etapas de processamento).

Como o arquivo parquet localizado dentro do Minio não funciona de forma semelhante a um banco de dados, não sendo possível incluir na tabela inteira somente uma única linha de forma incremental, a cada inclusão de uma nova linha, é necessário selecionar o conteúdo atual da tabela para a memória, inserir mais uma linha e escrever novamente toda a tabela no lake. Para evitar todos os problemas que isso pode gerar, a tabela é particionada por data, sendo assim, a lógica de sobreescrita é feita dentro de uma data, e não em todo o histórico da tabela, melhorando a eficiência do processo.

Por fim, com todo o código já feito e as eventuais configurações já predefinidas no Docker Compose e demais arquivos de configuração, basta agora subir todos os containers, e a partir de novas eventos de (INSERT, UPDATE, DELETE) nas tabelas predefinidas no banco, esta será replicada em tempo real para o lake, com as eventuais transformações necessárias, pronto para consumo.

**O projeto concentra-se no desenvolvimento da lógica e engenharia, e não necessariamente na segurança. Para simplificar o código, não serão criadas variáveis de ambiente para as credenciais, que, em vez disso, serão diretamente incorporadas no código. 

## Pré-Requisitos

Antes de prosseguir com este projeto, é necessário ter o Docker Desktop instalado em sua máquina local. 

## Executando o Projeto

Siga os passos abaixo para executar este projeto:
1. Copie o diretório do projeto para uma pasta local em seu computador.
2. Abra o terminal do seu computador e mova até o diretório do projeto.
3. Crie a imagem do container do Python executando o seguinte comando: `docker build -t python-kafka .`
4. Crie os containers com o seguinte comando: `docker-compose up -d. `


