from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import *

spark = (SparkSession.builder
         .config("spark.jars","""/usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,
                                 /usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,
                                 /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,
                                 /usr/local/airflow/jars/hadoop-aws-3.2.2.jar,
                                 /usr/local/airflow/jars/delta-core_2.12-1.2.1.jar,
                                 /usr/local/airflow/jars/postgresql-42.3.3.jar""")
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "ZiVYnHFvBt2HumCAXmQG")
         .config("spark.hadoop.fs.s3a.secret.key", "oVNgBEZ5PISLiwF25fJkoazHGXRFB7mJeiBcVygd")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
        )


# Definindo o diretório de origem e o diretório de destino
input_path_format = "s3a://bronze/yellow_taxi_files/yellow_tripdata_2023-{month:02d}.parquet"
output_path = "s3a://silver/prd_yellow_taxi_table"

# Loop para iterar sobre os meses de 01 a 05
for month in range(1, 6):
    # Formatando o caminho do arquivo para o mês atual
    parquet_path = input_path_format.format(month=month)

    # Carregar os arquivos Parquet
    df = spark.read.parquet(parquet_path)

    # Caso haja discrepâncias de tipo entre os arquivos, faça o cast necessário
    df = (
        df
        .withColumn("VendorID", F.col("VendorID").cast(IntegerType()))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("RatecodeID", F.col("RatecodeID").cast(IntegerType()))
        .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast(StringType()))
        .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
        .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(LongType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("extra", F.col("extra").cast(DoubleType()))
        .withColumn("mta_tax", F.col("mta_tax").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast(DoubleType()))
        .withColumn("airport_fee", F.col("airport_fee").cast(DoubleType()))
    )

    # Selecionando apenas as colunas necessárias
    df = (
        df.select(
        'VendorID',
        'passenger_count',
        'total_amount',
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime'
        )
        .filter(
                (F.year(df["tpep_pickup_datetime"]) == 2023) & 
                (F.month(df["tpep_pickup_datetime"]).between(1, 5))
            )
      )

    # Salvando os dados no formato Delta no path final com append
    (
        df
        .write
        .format('delta')
        .mode('append')  # Usando append para adicionar ao Delta Lake
        .save(output_path)
    )