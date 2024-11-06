import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
#import urllib3
#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



# Configuraciones iniciales
url_nifi_api = "https://nifi-devtool-qa-d4m.ms4m.com/nifi-api"
username = "user"
password = "LOPeRYteiMInGlINIOunDIBl"
process_group_id = "80e2eaec-0191-1000-0293-8ae752856f69"  # Reemplaza con el ID de tu Process Group

# Payload para obtener el token
access_payload = {
    "username": username,
    "password": password
}

# Función para obtener el token de autenticación
def get_token():
    auth_url = f"{url_nifi_api}/access/token"
    response = requests.post(auth_url, data=access_payload, verify=False)  # omitimos la verificación SSL
    response.raise_for_status()  # Lanza un error si la respuesta no es exitosa
    token = response.text
    print(f"Conexión exitosa. Token obtenido: {token}")
    return token

# Función para obtener los detalles del grupo de procesos
def get_process_group_details(process_group_id):
    token = get_token()
    details_url = f"{url_nifi_api}/process-groups/{process_group_id}"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(details_url, headers=headers, verify=False)  # omitimos la verificación SSL
    response.raise_for_status()
    process_group_details = response.json()
    return process_group_details

# Función para obtener el estado del grupo de procesos
def get_process_group_status():
    token = get_token()
    status_url = f"{url_nifi_api}/flow/process-groups/{process_group_id}/status"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(status_url, headers=headers, verify=False)  # omitimos la verificación SSL
    response.raise_for_status()
    process_group_status = response.json()
    print(f"Estado del grupo de procesos: {process_group_status}")
    return process_group_status

# Función para iniciar el grupo de procesos
def start_process_group():
    process_group_details = get_process_group_details(process_group_id)
    client_id = process_group_details['revision']['clientId']
    version = process_group_details['revision']['version']

    status = get_process_group_status()  # Usa el endpoint GET /flow/process-groups/{id}/status
    print(f"Estado del grupo de procesos recibido: {status}")  # Imprimir el JSON completo

    # Verifica si algún procesador dentro del grupo está detenido
    processors = status.get('processGroupStatus', {}).get('processorStatusSnapshots', [])
    all_running = all(
        processor.get('processorStatusSnapshot', {}).get('runStatus') == 'Running'
        for processor in processors
    )

    # Si al menos un procesador no está en ejecución, iniciamos el grupo de procesos
    if not all_running:
        token = get_token()
        start_url = f"{url_nifi_api}/flow/process-groups/{process_group_id}"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        payload = {
            "revision": {
                "clientId": client_id,
                "version": version
            },
            "state": "RUNNING"
        }
        response = requests.put(start_url, headers=headers, json=payload, verify=False)  # omitimos la verificación SSL
        response.raise_for_status()
        print("Grupo de procesos iniciado correctamente.")
    else:
        print("El grupo de procesos ya está en ejecución.")



# Definir el DAG de Airflow
with DAG(
    dag_id="5conexion_run_group_procesador_nifi",
    description="DAG para iniciar un grupo de procesos de NiFi",
    start_date=datetime(2024, 9, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Tarea para obtener los detalles del grupo de procesos de NiFi
    get_process_group_details_task = PythonOperator(
        task_id='get_process_group_details',
        python_callable=get_process_group_details,
        op_kwargs={'process_group_id': process_group_id},
    )

    # Tarea para obtener el estado del grupo de procesos de NiFi
    get_process_group_status_task = PythonOperator(
        task_id='get_process_group_status_task',
        python_callable=get_process_group_status,
    )

    # Tarea para iniciar el grupo de procesos de NiFi
    start_process_group_task = PythonOperator(
        task_id='start_process_group_task',
        python_callable=start_process_group,
    )


    # Establecer dependencias
    get_process_group_details_task >> get_process_group_status_task >> start_process_group_task