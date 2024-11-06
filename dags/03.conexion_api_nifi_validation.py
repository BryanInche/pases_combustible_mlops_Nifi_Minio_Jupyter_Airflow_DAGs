import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Configuraciones iniciales
#url_nifi_api = "http://nifi.nifi:8443/nifi-api"
url_nifi_api = "https://nifi-devtool-qa-d4m.ms4m.com/nifi-api"

username = "user"
password = "LOPeRYteiMInGlINIOunDIBl"

# Payload para obtener el token
access_payload = {
    "username": username,
    "password": password
}

# Función para validar la conexión a la API de NiFi
def validate_nifi_connection():
    try:
        # Obtener el token de autenticación
        auth_url = f"{url_nifi_api}/access/token"
        response = requests.post(auth_url, data=access_payload, verify=False)
        response.raise_for_status()  # Lanza un error si la respuesta no es exitosa
        token = response.text
        print(f"Conexión exitosa. Token obtenido: {token}")
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Error al conectarse a la API de NiFi: {e}")

# Definir el DAG de Airflow
with DAG(
    dag_id="3conexion_api_nifi_validation",
    description="DAG para validar la conexión a la API de NiFi",
    start_date=datetime(2024, 9, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Tarea para validar la conexión a la API de NiFi
    validate_connection = PythonOperator(
        task_id='validate_connection',
        python_callable=validate_nifi_connection,
    )

    validate_connection
