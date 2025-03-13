from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import requests
from datetime import datetime, timedelta

project_id = 'velib-mspr'
dataset_id = 'raw_data'
table_station_info = 'stations_infos'
table_station_status = 'station_status'
gcs_bucket = 'velib-mspr-bucket-raw-data-eu'

default_args = {
    'owner': 'eid',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 13),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'velib_data_to_bigquery',
    default_args=default_args,
    description="Extraction des données Velib et chargement dans Google Bigquery.",
    schedule_interval=timedelta(seconds=90),
    catchup=False,
)

def fetch_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()['data']['stations']

def extract_and_save_data(**kwargs):
    url_station = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
    url_status = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
    
    execution_date = kwargs.get('execution_date', datetime.now())
    timestamp = execution_date.strftime('%Y-%m-%d_%H-%M-%S')
    # execution_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S')

    # Fetch data
    stations_infos = fetch_data(url_station)
    stations_status = fetch_data(url_status)

    # Flatten num_bikes_available_types by extracting mechanical and ebike counts
    for station in stations_status:
        mechanical = 0
        ebike = 0
        # Loop over the list and sum the available bikes (if there might be multiple entries)
        for bike_type in station.get("num_bikes_available_types", []):
            mechanical += bike_type.get("mechanical", 0)
            ebike += bike_type.get("ebike", 0)
        
        # Create new top-level fields
        station["mechanical"] = mechanical
        station["ebike"] = ebike
        
        # Optionally, remove the original nested field if not needed
        station.pop("num_bikes_available_types", None)

    # Convert to DataFrames and save as Parquet
    df_station_status = pd.DataFrame(stations_status)
    df_stations_infos = pd.DataFrame(stations_infos)

    df_station_status['execution_date'] = timestamp



    # Save as Parquet
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

    # stations_parquet_path = f'/tmp/stations_infos_{timestamp}.parquet'
    # status_parquet_path = f'/tmp/status_{timestamp}.parquet'
    
    # Upload stations_infos
    stations_parquet_path = f'/tmp/stations_infos_{timestamp}.parquet'
    df_stations_infos.to_parquet(stations_parquet_path, index=False)
    gcs_hook.upload(
        bucket_name=gcs_bucket,
        object_name=f'stations_infos_{timestamp}.parquet',
        filename=stations_parquet_path,
        mime_type='application/octet-stream',
    )

    # Upload status
    status_parquet_path = f'/tmp/status_{timestamp}.parquet'
    df_station_status.to_parquet(status_parquet_path, index=False)
    gcs_hook.upload(
        bucket_name=gcs_bucket,
        object_name=f'status_{timestamp}.parquet',
        filename=status_parquet_path,
        mime_type='application/octet-stream',
    )

    kwargs['ti'].xcom_push(key='stations_path', value=f"gs://{gcs_bucket}/stations_infos_{timestamp}.parquet")
    kwargs['ti'].xcom_push(key='status_path', value=f"gs://{gcs_bucket}/status_{timestamp}.parquet")


extract_task = PythonOperator(
    task_id='Extract_velib_data',
    python_callable=extract_and_save_data,
    provide_context=True,
    dag=dag,
)



load_stations_task = BigQueryInsertJobOperator(
    task_id='load_stations_infos',
    configuration={
        'load': {
            'sourceUris': ['{{ ti.xcom_pull(key="stations_path") }}'],
            'destinationTable': {
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': table_station_info,
            },
            'sourceFormat': 'PARQUET',  # Changed from JSON
            'writeDisposition': 'WRITE_APPEND',
            'createDisposition': 'CREATE_IF_NEEDED',
            # Schema is optional for Parquet (BigQuery auto-detects it)
        }
    },
    dag=dag,
)

load_status_task = BigQueryInsertJobOperator(
    task_id='load_stations_status',
    configuration={
        'load': {
            'sourceUris': ['{{ ti.xcom_pull(key="status_path") }}'],
            'destinationTable': {
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': table_station_status,
            },
            'sourceFormat': 'PARQUET',
            'writeDisposition': 'WRITE_APPEND',
            'createDisposition': 'CREATE_IF_NEEDED',
            'schema': {
                'fields': [
                    {'name': 'station_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                    {'name': 'num_bikes_available', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'numBikesAvailable', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'mechanical', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'ebike', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'num_docks_available', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'numDocksAvailable', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'is_installed', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'is_returning', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'is_renting', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'last_reported', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'stationCode', 'type': 'STRING', 'mode': 'REQUIRED'},
                    {'name': 'station_opening_hours', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'execution_date', 'type': 'STRING', 'mode': 'NULLABLE'},
                ]
            },
        }
    },
    dag=dag,
)

extract_task >> [load_stations_task, load_status_task]