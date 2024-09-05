# Librerias para trabajar con Airflow Dags y todos los trabajos
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from io import BytesIO
from minio import Minio
import urllib3
from pyspark.sql import SparkSession
from delta import *
import numpy as np

#PASO 1 :  Define una funcion de Python Operatior si es Necesario para tu DAG

# 1.1 Función para leer datos desde MinIO
def read_data_from_minio(ti):

    client = Minio(
        "192.168.25.223", #Dirección del servidor MinIO
        #"minio.minio-user.svc.cluster.local:80",  # Dirección del servidor MinIO
        access_key="LjPODFkcEC5NwpsNCGWX",         # Clave de acceso
        secret_key="PIGKHRP0HoQF4yrkcumtXJamnRTxtlwpHwTvKtn2",  # Clave secreta
        secure=False,  # Cambia a True si usas HTTPS
        http_client=urllib3.PoolManager(cert_reqs='CERT_NONE')  # Ignora la verificación del certificado
    )

    bucket_name = 'hudbayraw2'
    object_name = 'bd_hubbay_pases_all.parquet'

    # Obtener el archivo Parquet del bucket
    response = client.get_object(bucket_name, object_name)
    data = response.read()

    # Convertir los datos leídos en un DataFrame de Pandas
    parquet_buffer = BytesIO(data)
    df = pd.read_parquet(parquet_buffer)

    # Utilizar ti.xcom_push para pasar los datos al siguiente task
    ti.xcom_push(key='minio_data', value=df)

    # (Opcional) Procesar o almacenar el DataFrame como necesites
    #return df.head()


# 1.2 Función para preprocesar datos y guardar como tabla Delta en MinIO
def preprocess_and_save_delta(ti):
    # Recuperar los datos desde XComs
    datos = ti.xcom_pull(key='minio_data', task_ids='read_data_minio')  #'read_data_minio'  es el task_id definido mas abajo que pertenece a la funcion read_data_from_minio

    # 1. Tratamiento de valores nulos
    valores_nulos = datos.isnull().sum()
    porcentaje_nulos = (valores_nulos / len(datos)) * 100
    columnas_a_eliminar = porcentaje_nulos[porcentaje_nulos > 80].index
    datos = datos.drop(columnas_a_eliminar, axis=1)

    # 2. Configuración de Spark y Delta
    S3_ACCESS_KEY = "nIfbWDx9lhnt5Y2I58bh"
    S3_BUCKET = "hudbayprocessed2"
    S3_SECRET_KEY = "OjbcZm24KUk8jlEPqedS7FzmvHilnwkN5mK77E7e"
    S3_ENDPOINT = "192.168.25.223"


    # Configuración de la sesión de Spark para guardar como tabla Delta en MinIO
    spark = SparkSession \
        .builder \
        .appName("AppDeltaMinio9") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.cores", "2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0,com.amazonaws:aws-java-sdk-pom:1.12.720") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .getOrCreate()

    spark_df = spark.createDataFrame(datos)
    spark_df.write.format("delta").mode("overwrite").save(f"s3a://hudbayprocessed2/tabladelta_procesing_hudbay_bryan")

# PASO 2 : Definir el DAG (argumentos basicos)
# Definir el DAG
with DAG(
    'dag_modelo_pasesvf',
    description='DAG-ETL-Modelo-Pases',
    schedule_interval='@daily',  # Configura el intervalo según tus necesidades
    start_date=datetime(2024, 8, 25),  # Fecha y hora de inicio yyyy/mm/dd
    catchup=False,
) as dag:
    

# PASO 3 : Definir los Task del DAG

    # Crear la tarea de lectura
    read_task = PythonOperator(
        task_id='read_data_minio',
        python_callable=read_data_from_minio,
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_save_delta',
        python_callable=preprocess_and_save_delta,
    )

# PASO 4 : Secuenciar las tareas del DAG(basados en los task)
    read_task >> preprocess_task
