from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime

# Paso 1 . Define a ML function that will be executed as a PythonOperator task

kwargs = {
    'start_date': '2024-01-01',
    'end_date': '2024-02-01',
    'fleets': '7',
}

start_date = '2024-01-01'
end_date = '2024-02-01'
fleet = '5,6,7'

credential_minio = {'endpoint':"192.168.25.223",'access_key':'c9iXL6uoEu8r35odfMLV','secret_key':'r3Wx21EmA41gB3mH65mvBVG9sH3lIMPwSmD0WMtI'}
credential_servidor = {'endpoint': "192.168.25.223", 'access_key': 'c9iXL6uoEu8r35odfMLV', 'secret_key': 'r3Wx21EmA41gB3mH65mvBVG9sH3lIMPwSmD0WMtI'}

# Paso 2. Define the DAG
with DAG(
    dag_id="6conexion_dag_volumen_python_with_params",
    description="A DAG for executing a volumen Minio",
    start_date=datetime(2024, 8, 27),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Define the tasks
    h4m_ingesta = BashOperator(
        task_id="h4m_ingesta",
        bash_command=f"""
            python /opt/airflow/pythonlib/volumen_script_h4m_v2.py {start_date} {end_date} {fleet}
            """
    )

    # h4m_ingesta = PythonOperator(
    #     task_id='task_ingesta_h4m',
    #     python_callable=ingesta_h4m,
    #     op_kwargs=kwargs
    # )

    # Set task dependencies
    h4m_ingesta