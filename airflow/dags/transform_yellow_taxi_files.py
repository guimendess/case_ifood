import os
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

os.environ["JAVA_HOME"] = Variable.get('JAVA_HOME')

default_args = {
    'owner': 'ifood',
    'start_date': datetime(2024, 11, 1)
}

dag = DAG(dag_id='dag_transform_yellow_taxi_files',
          default_args=default_args,
          schedule_interval='0 3 * * *',
          tags=['SILVER']
      )

start_dag = DummyOperator(
                task_id='start_dag',
                dag=dag
                )

task = SparkSubmitOperator(
                          task_id='transform_yellow_taxi_files',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar,\
                                /usr/local/airflow/jars/postgresql-42.3.3.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/spark_scripts/transform_yellow_taxi_files.py',
                          dag=dag
                      )


dag_finish = DummyOperator(
                 task_id='dag_finish',
                 dag=dag
                 )


start_dag >> task >> dag_finish