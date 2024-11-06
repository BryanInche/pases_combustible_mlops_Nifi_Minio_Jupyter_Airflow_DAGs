from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Definir función que verificará la instalación de SimPy
def check_simpy_installed():
    try:
        import simpy
        print("SimPy está instalado correctamente.")
    except ImportError:
        raise ImportError("SimPy no está instalado.")

# Definir el DAG
with DAG(
    '9.dag_add_librarie',
    description='DAG-Modelo-Pases',
    schedule_interval='@daily',  # Configura el intervalo según tus necesidades
    start_date=datetime(2024, 10, 1),  # Fecha y hora de inicio yyyy/mm/dd
    catchup=False,
) as dag:
    
# PASO 3 : Definir los Task del DAG

    # Crear la tarea de lectura
    read_task = PythonOperator(
        task_id='check_librarie',
        python_callable=check_simpy_installed,
    )

# PASO 4 : Secuenciar las tareas del DAG(basados en los task)
    read_task