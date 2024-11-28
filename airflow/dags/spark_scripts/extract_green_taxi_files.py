from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
import os
import requests
import re
from bs4 import BeautifulSoup
from minio import Minio
from urllib.parse import urlparse
from minio import Minio

spark = (SparkSession.builder
         .config("spark.jars","""/home/jovyan/jars/aws-java-sdk-core-1.11.534.jar,
                                 /home/jovyan/jars/aws-java-sdk-dynamodb-1.11.534.jar,
                                 /home/jovyan/jars/aws-java-sdk-s3-1.11.534.jar,
                                 /home/jovyan/jars/hadoop-aws-3.2.2.jar,
                                 /home/jovyan/jars/postgresql-42.3.3.jar""")
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "ZiVYnHFvBt2HumCAXmQG")
         .config("spark.hadoop.fs.s3a.secret.key", "oVNgBEZ5PISLiwF25fJkoazHGXRFB7mJeiBcVygd")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
        )


# URL da página onde os arquivos Parquet estão disponíveis
url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'

# Fazer a requisição HTTP para a página
response = requests.get(url)

# Criar o BeautifulSoup para analisar o conteúdo HTML
soup = BeautifulSoup(response.content, 'html.parser')

# Encontrar todos os links na página
links = soup.find_all('a', href=True)

# Filtrar as URLs que terminam com ".parquet" - alguns arquivos foram salvos com espaço no final da string da url.
parquet_urls = [link['href'] for link in links if link['href'].endswith('.parquet') or link['href'].endswith('.parquet ')]

#Removendo os espaços extras nos objetos do array
parquet_urls = [url.strip() for url in parquet_urls]


# Função para filtrar as URLs
def filter_parquet_urls(urls):
    # Regex para encontrar o nome 'green_tripdata' e datas de janeiro a maio de 2023
    filtered_urls = [
        url for url in urls
        if "green_tripdata" in url and re.search(r'2023-(0[1-5])\.parquet$', url)
    ]
    return filtered_urls

# Aplicar a filtragem
filtered_urls = filter_parquet_urls(parquet_urls)

# Exibir as URLs filtradas
for url in filtered_urls:
    print(url)

# Verificando se o bucket existe
try:
    minio_client = Minio(
        "172.18.0.5:9000",
        access_key="ZiVYnHFvBt2HumCAXmQG",
        secret_key="oVNgBEZ5PISLiwF25fJkoazHGXRFB7mJeiBcVygd",
        secure=False
    )
    buckets = minio_client.list_buckets()
    for bucket in buckets:
        print(f"Bucket: {bucket.name} criado em {bucket.creation_date}")
except Exception as e:
    print(f"Erro ao conectar ao MinIO: {e}")


# Configuração do MinIO (endpoint local)
minio_endpoint = "http://172.18.0.5:9000"
bucket_name = "bronze"

# Seta os dados do bucket
minio_client = Minio(
    "172.18.0.5:9000",  # MinIO endpoint
    access_key="ZiVYnHFvBt2HumCAXmQG",  # Access key
    secret_key="oVNgBEZ5PISLiwF25fJkoazHGXRFB7mJeiBcVygd",  # Secret key
    secure=False  # Sem HTTPS
)

# Baixar os arquivos e salvar no MinIO
for parquet_url in filtered_urls:
    # Nome do arquivo local
    file_name = os.path.basename(parquet_url)
    local_path = f'/tmp/{file_name}'

    # Baixar o arquivo Parquet
    response = requests.get(parquet_url)
    with open(local_path, 'wb') as f:
        f.write(response.content)

    print(f'{file_name} baixado com sucesso.')

    # Definir o diretório no bucket (green_taxi_files)
    object_name = f"green_taxi_files/{file_name}"

    # Subir para o MinIO dentro do diretório green_taxi_files
    with open(local_path, 'rb') as f:
        minio_client.put_object(bucket_name, object_name, f, os.stat(local_path).st_size)

    print(f'{file_name} salvo no MinIO em {bucket_name}/{object_name}')

    # Remover o arquivo local temporário
    os.remove(local_path)