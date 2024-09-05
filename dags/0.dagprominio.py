# Librerias para trabajar con Airflow Dags y todos los trabajos
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from io import BytesIO
from minio import Minio
import urllib3

#PASO 1 :  Define una funcion de Python Operatior si es Necesario para tu DAG

# Función para leer datos desde MinIO y convertirlos a un DataFrame de Pandas
def read_data_from_minio():

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

    # (Opcional) Procesar o almacenar el DataFrame como necesites
    return df.head(2)



# PASO 2 : Definir el DAG (argumentos basicos)
# Definir el DAG
with DAG(
    'dag_lectura_minio',
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

# PASO 4 : Secuenciar las tareas del DAG(basados en los task)
    read_task
