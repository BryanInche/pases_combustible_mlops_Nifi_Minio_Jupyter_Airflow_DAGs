from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datahub_provider.entities import Dataset
from datahub_provider.hooks.datahub import DatahubKafkaHook
from datetime import datetime

from io import BytesIO
from minio import Minio
import urllib3
import pandas as pd
from pyspark.sql import SparkSession
from delta import *

import psycopg2
import time

import numpy as np
from scipy import stats
import os
import re
import ast

class ConexionPostgreSQL:
    def __init__(self, host, database, user, password):
        self.conn = None
        self.connect(host, database, user, password)

    def connect(self, host, database, user, password):
        """Establece la conexión a la base de datos."""
        try:
            self.conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
        except (Exception, psycopg2.Error) as error:
            print("Error al conectar a la base de datos:", error)

    def ejecutar_consulta(self, consulta:str, parametro:tuple):
        """Ejecuta una consulta SQL y devuelve los resultados en formato dataframe."""
        inicio = time.time()
        try:
            cursor = self.conn.cursor()
            time_zone_query = "SET TIME ZONE 'America/Lima';"
            cursor.execute(time_zone_query)
            if parametro is None:
                cursor.execute(consulta)
            else:
                cursor.execute(consulta, parametro)
            resultados = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
            fin = time.time()
            tiempo_ejecucion = fin - inicio
            cursor.close()
            print("Tiempo de ejecución:", tiempo_ejecucion, "segundos")
            return resultados
        except (Exception, psycopg2.Error) as error:
            print("Error al ejecutar la consulta:", error)

    def cerrar_conexion(self):
        """Cierra la conexión a la base de datos."""
        if self.conn:
            self.conn.close()
            print("Conexión cerrada")


def extract_data():
    print('Ingreso a la funcion extract_data ')

    config = {
        'host': '192.168.3.130',
        'database': 'ControlSenseDB',
        'user': 'postgres',
        'password': 'postgres'
    }

    db = ConexionPostgreSQL(**config)

    consulta = '''
            SELECT
            *
            FROM public.ts_equipos
            ORDER BY id ASC
            LIMIT 10

        '''

    records = db.ejecutar_consulta(consulta, None)
    return records

def save_lineage():
    dh_hook = DatahubKafkaHook()
    dataset = Dataset(
        name='ta_prisma_hp',
        platform='postgres',
        env='PROD',
    )
    dh_hook.emit_mce(dataset)   

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('datahub_lineage_dag',
    default_args=default_args,
    schedule_interval='@daily') as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    lineage_task = PythonOperator(
        task_id='save_lineage',
        python_callable=save_lineage
    )

    extract_task >> lineage_task
