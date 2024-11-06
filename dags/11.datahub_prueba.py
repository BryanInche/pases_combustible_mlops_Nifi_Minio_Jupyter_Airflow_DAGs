from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datahub_provider.entities import Dataset
from datahub_provider.hooks.datahub import DatahubGenericHook

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'postgres_data_ingestion',
    default_args=default_args,
    description='Ingest data from PostgreSQL and capture lineage with DataHub',
    schedule_interval=None,
)

# Define the SQL query
sql_query = """
SELECT * FROM public.ts_equipos
ORDER BY id ASC LIMIT 10;
"""

# Task to execute the SQL query
run_query = PostgresOperator(
    task_id='run_query',
    postgres_conn_id='Postgres',
    sql=sql_query,
    dag=dag,
)

# Task to capture lineage
def capture_lineage():
    hook = DatahubGenericHook(datahub_conn_id='DAG_Datahub')
    upstream_urn = hook.make_dataset_urn(platform='postgres', name='ControlSenseDB.public.ts_equipos', env='PROD')
    downstream_urn = hook.make_dataset_urn(platform='postgres', name='ControlSenseDB.public.processed_table', env='DEV')
    hook.emit_mcp(
        entity_type='dataset',
        entity_urn=downstream_urn,
        aspect_name='upstreamLineage',
        aspect={
            'upstreams': [{'dataset': upstream_urn, 'type': 'TRANSFORMED'}]
        }
    )

capture_lineage_task = PythonOperator(
    task_id='capture_lineage',
    python_callable=capture_lineage,
    dag=dag,
)

run_query >> capture_lineage_task
