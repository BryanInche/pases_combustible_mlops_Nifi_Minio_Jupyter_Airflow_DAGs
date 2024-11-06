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
import re

#PASO 1 :  Define una funcion de Python Operatior si es Necesario para tu DAG

# 1.1 Función para leer datos desde MinIO
def read_data_from_minio(ti):

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

    # Leer la tabla Delta desde MinIO
    delta_df = spark.read.format("delta").load("s3a://hudbayprocessed2/tabladelta_preprocesing_flujod4m")
    datos = delta_df.toPandas()

    # Utilizar ti.xcom_push para pasar los datos al siguiente task
    ti.xcom_push(key='minio_data', value=datos)

# 1.2 Función para preprocesar datos y guardar como tabla Delta en MinIO
def features_engineer(ti):
    # Recuperar los datos desde XComs
    datos = ti.xcom_pull(key='minio_data', task_ids='read_data_minio')  #'read_data_minio'  es el task_id definido mas abajo que pertenece a la funcion read_data_from_minio

    datos = datos[['tiempo_inicio_carga_carguio', 'tiempo_esperando_carguio', 'tiempo_inicio_cambio_estado_camion','tonelaje_segun_computadora',
    'capacidad_en_peso_equipo_carguio']]

    # 2. Configuración de Spark y Delta
    S3_ACCESS_KEY = "c9iXL6uoEu8r35odfMLV"
    S3_BUCKET = "hudbaypresentation"
    S3_SECRET_KEY = "r3Wx21EmA41gB3mH65mvBVG9sH3lIMPwSmD0WMtI"
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
    spark_df.write.format("delta").mode("overwrite").save(f"s3a://hudbaypresentation/tabladelta_presentation_flujod4m")


