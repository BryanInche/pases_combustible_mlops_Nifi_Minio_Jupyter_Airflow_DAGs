import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Configuraciones iniciales
url_nifi_api = "https://nifi-devtool-qa-d4m.ms4m.com/nifi-api"
username = "user"
password = "LOPeRYteiMInGlINIOunDIBl"
processor_id = "b4f78386-0191-1000-e7aa-3a26f28cb6e7"  # Reemplaza con el ID de tu procesador

# Payload para obtener el token
access_payload = {
    "username": username,
    "password": password
}

def get_token():
    auth_url = f"{url_nifi_api}/access/token"
    response = requests.post(auth_url, data=access_payload, verify=False) #omitimos la verificacion ssl
    response.raise_for_status()  # Lanza un error si la respuesta no es exitosa
    token = response.text
    print(f"Conexión exitosa. Token obtenido: {token}")
    return token

def get_processor_details(processor_id):
    token = get_token()
    details_url = f"{url_nifi_api}/processors/{processor_id}"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(details_url, headers=headers, verify=False) #omitimos la verificacion ssl
    response.raise_for_status()
    processor_details = response.json()
    return processor_details

def get_processor_status():
    token = get_token()
    status_url = f"{url_nifi_api}/flow/processors/{processor_id}/status"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(status_url, headers=headers, verify=False) #omitimos la verificacion ssl
    response.raise_for_status()
    processor_status = response.json()
    print(f"Estado del procesador: {processor_status}")
    return processor_status

def start_processor():
    # Obtiene los detalles del procesador dinámicamente
    processor_details = get_processor_details(processor_id)
    client_id = processor_details['revision']['clientId']
    version = processor_details['revision']['version']

    status = get_processor_status()
    run_status = status['processorStatus']['aggregateSnapshot']['runStatus']
    
    if run_status != 'Running':
        token = get_token()
        start_url = f"{url_nifi_api}/processors/{processor_id}"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        payload = {
            "revision": {
                "clientId": client_id,  # Usa el clientId dinámico,
                "version": version,     # Usa la versión dinámica 
            },
            "component": {
                "id": processor_id,
                "state": "RUNNING"
            }
        }
        response = requests.put(start_url, headers=headers, json=payload, verify=False) #omitimos la verificacion ssl
        response.raise_for_status()
        print("Procesador iniciado correctamente.")
    else:
        print("El procesador ya está en ejecución.")



# Definir el DAG de Airflow
with DAG(
    dag_id="4conexion_run_procesador_nifi",
    description="DAG para iniciar el procesador de NiFi",
    start_date=datetime(2024, 9, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Tarea para obtener el estado del procesador de NiFi
    get_status_processor = PythonOperator(
        task_id='get_processor_details',
        python_callable=get_processor_details,
        op_kwargs={'processor_id': processor_id},
    )

    # Tarea para obtener el estado del procesador de NiFi
    get_status_task = PythonOperator(
        task_id='get_processor_status_task',
        python_callable=get_processor_status,
    )

    # Tarea para iniciar el procesador de NiFi si no está en ejecución
    start_processor_task = PythonOperator(
        task_id='start_processor_task',
        python_callable=start_processor,
    )

    # Establecer dependencias
    get_status_processor >> get_status_task >> start_processor_task





