from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime

# Paso 1 . Define a ML function that will be executed as a PythonOperator task

# Paso 2. Define the DAG
with DAG(
    dag_id="2conexion_dag_volumen_python",
    description="A DAG for executing a volumen Minio",
    start_date=datetime(2024, 8, 27),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # # Define the tasks
    # task_bash = BashOperator(
    #     task_id="bash_task",
    #     bash_command="""
    #         echo "Executing bash script..."
    #         # running a python script
    #         python /opt/airflow/pythonlib/volumen_script_pythonv2.py
    #     """
    # )
    # Define the tasks
    task_bash = BashOperator(
        task_id="bash_task",
        bash_command="""
            echo "Executing bash script..."
            # running a python script
            pip list --format=columns

        """
    )

    # Set task dependencies
    task_bash

