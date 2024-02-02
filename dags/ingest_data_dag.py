import requests
from datetime import datetime, timedelta


from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator



# config varibales
GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "tlc-trips-data-analytics"
BUCKET = "tlc-trip-records"
STAGING_DATASET = "staging"
MAIN_DATASET = "tlc_trip_records"
# change this varible to a dynamic date from your last run
PERIOD = '2023-06'


# Default arguments
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@weekly'
}

@dag(dag_id="incremental_ingestion_pipe", default_args=default_args, catchup=False)
def task_flow():
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline')

    @task(task_id="extract_and_load_trips_to_gcs")
    def extract_and_load_trips_to_gcs():
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{PERIOD}.parquet"
        response = requests.get(url)
        response.raise_for_status() # raise exceptions if errors exist
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        gcs_hook.upload(bucket_name=BUCKET, data=response.content, object_name=f"yellow_tripdata_{PERIOD}.parquet")

    @task(task_id="extract_and_load_weather_to_gcs")
    def extract_and_load_weather_to_gcs():
        url = f"https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{PERIOD[:4]}/72505394728.csv"
        response = requests.get(url)
        response.raise_for_status() # raise exceptions if errors exist
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        gcs_hook.upload(bucket_name=BUCKET, data=response.text, object_name=f"weather_{PERIOD[:4]}.csv")

    extracted_from_api = DummyOperator(
        task_id = 'extracted_from_api')
    
    load_trips_to_staging = GCSToBigQueryOperator(
        task_id="load_trips_to_staging",
        bucket=BUCKET,
        source_objects=[f"yellow_tripdata_{PERIOD}.parquet"],
        destination_project_dataset_table=f"{STAGING_DATASET}.trips",
        source_format='PARQUET',
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    load_weather_to_staging = GCSToBigQueryOperator(
        task_id="load_weather_to_staging",
        bucket=BUCKET,
        source_objects=[f"weather_{PERIOD[:4]}.csv"],
        destination_project_dataset_table=f"{STAGING_DATASET}.weather",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    loaded_to_staging = DummyOperator(
        task_id = 'loaded_to_staging')
    
    merge_trips = BigQueryInsertJobOperator(
        task_id='merge_trips',
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": './sql/merge_trips.sql',
                "useLegacySql": False
            }
        }
    )

    merge_weather = BigQueryInsertJobOperator(
        task_id='merge_weather',
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": './sql/merge_trips.sql',
                "useLegacySql": False
            }
        }
    )

    merged_into_main_tables = DummyOperator(
        task_id = 'merged_into_main_tables')

    end_pipeline = DummyOperator(
        task_id = 'end_pipeline')
    
    start_pipeline >> [extract_and_load_trips_to_gcs(), extract_and_load_weather_to_gcs()] >> extracted_from_api
    extracted_from_api >> [load_trips_to_staging, load_weather_to_staging] >> loaded_to_staging
    loaded_to_staging >> [merge_trips, merge_weather] >> merged_into_main_tables >> end_pipeline

task_flow()