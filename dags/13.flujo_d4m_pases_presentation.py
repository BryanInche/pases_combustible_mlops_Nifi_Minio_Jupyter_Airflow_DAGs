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
def read_data_from_stage(ti):

    S3_ACCESS_KEY = "9oWEmRixKfcTwLxUR7a8"
    S3_BUCKET = "stage3"
    S3_SECRET_KEY = "ZlSe6P52cGi1XV5eECqXIbsoTVtmTEqPk0xguGel"
    S3_ENDPOINT = "192.168.25.223"
    #S3_ENDPOINT = "minio.minio-user.svc.cluster.local:80"

    #hadoop_common_jar = "/home/jovyan/jars/hadoop-client-api-3.3.4.jar,/home/jovyan/jars/hadoop-client-runtime-3.3.4.jar,/home/jovyan/jars/hadoop-common-3.3.4.jar"
    spark = SparkSession \
        .builder \
        .appName("AppDeltaMinio9") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.cores", "2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0,com.amazonaws:aws-java-sdk-pom:1.12.720") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .getOrCreate()

    # Leer la tabla Delta desde MinIO
    delta_df = spark.read.format("delta").load("s3a://stage3/tabledelta_stage_flujod4m")

    datos = delta_df.toPandas()

    # Utilizar ti.xcom_push para pasar los datos al siguiente task
    ti.xcom_push(key='stage_data', value=datos)

# 1.2 Función para preprocesar datos y guardar como tabla Delta en MinIO
def features_engineer(ti):
    # Recuperar los datos desde XComs
    datos = ti.xcom_pull(key='stage_data', task_ids='read_data_stage')  #'read_data_minio'  es el task_id definido mas abajo que pertenece a la funcion read_data_from_minio

    datos = datos[['id_ciclo_acarreo', 'id_cargadescarga_ciclo', 'id_palas',
       'id_equipo_camion', 'id_ciclo_carguio', 'tonelaje_inicial_poligono_creado',
        'densidad_inicial_poligono_creado_tn/m3',
        'elevacion_poligono_metros',
        'id_material_dominante_en_poligono',
        'nombre_equipo_carguio',
        'capacidad_en_volumen_equipo_carguio_m3',
        'capacidad_en_peso_equipo_acarreo',
        'tiempo_carga',
        'cantidad_equipos_espera_al_termino_carga_pala',
        'horaini_turnocarga',
        'numero_pases_carguio', 'numero_pases_carguio2']]

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
    spark_df.write.format("delta").mode("overwrite").save(f"s3a://hudbaypresentation/tabladelta_presentation_flujod4m_v1")
    #mode("append"): Los nuevos datos se añaden (se acumulan) al archivo o tabla existente

# PASO 2 : Definir el DAG (argumentos basicos)
# Definir el DAG
with DAG(
    '13dag_flujod4m_pases_presentation',
    description='Presentation-D4M-Modelo-Pases',
    schedule_interval='@daily',  # Configura el intervalo según tus necesidades
    start_date=datetime(2024, 11, 1),  # Fecha y hora de inicio yyyy/mm/dd
    catchup=False,
) as dag:
    

# PASO 3 : Definir los Task del DAG

    # Crear la tarea de lectura
    read_stage_task = PythonOperator(
        task_id='read_data_stage',
        python_callable=read_data_from_stage,
    )

    preprocess_presentation_task = PythonOperator(
        task_id='presentation_save_delta',
        python_callable=features_engineer,
    )


# PASO 4 : Secuenciar las tareas del DAG(basados en los task)
    read_stage_task >> preprocess_presentation_task


