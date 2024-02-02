from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator
)

# config varibales
GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "tlc-trips-data-analytics"
BUCKET="tlc-trip-records"
STAGING_DATASET = "staging"
MAIN_DATASET = "tlc_trip_records"
LOCATION='europe-west10'

# default arguments
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# initiate dag
@dag(dag_id="create_data_model", default_args=default_args, catchup=False)
def task_flow():

    # create staging dataset
    create_staging_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_staging_dataset',
        dataset_id=STAGING_DATASET,
        project_id=PROJECT_ID,
        location=LOCATION
    )
    
    # create a dataset named tlc_trip_records that contain all relational tables
    create_main_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_main_dataset',
        dataset_id=MAIN_DATASET,
        project_id=PROJECT_ID,
        location=LOCATION
    )

    # create an empty table named "f_trips" with monthly partitioning based on the "tpep_dropoff_datetime" column
    create_f_trips_table = BigQueryCreateEmptyTableOperator(
        task_id='create_f_trips_table',
        project_id=PROJECT_ID,
        dataset_id=MAIN_DATASET,
        table_id='f_trips',
        gcs_schema_object=f'gs://{BUCKET}/schemas/trips_schema.json',
        time_partitioning={
            'field': 'tpep_dropoff_datetime', 
            'type': 'MONTH'  
        }
    )

    # create empty d_weather table
    create_d_weather_table = BigQueryCreateEmptyTableOperator(
        task_id='create_d_weather_table',
        project_id=PROJECT_ID,
        dataset_id=MAIN_DATASET,
        table_id = 'd_weather',
        gcs_schema_object=f'gs://{BUCKET}/schemas/weather_schema.json',
        time_partitioning={
            'field': 'date', 
            'type': 'MONTH'    
        }
    )

    # create d_locations table
    create_d_locations_table = GCSToBigQueryOperator(
        task_id='create_d_locations_table',
        bucket=BUCKET,
        source_objects=['locations.csv'],
        destination_project_dataset_table=f'{MAIN_DATASET}.d_locations'
    )

    # create d_payments table
    create_d_payments_table = GCSToBigQueryOperator(
        task_id='create_d_payments_table',
        bucket=BUCKET,
        source_objects=['payments.csv'],
        destination_project_dataset_table=f'{MAIN_DATASET}.d_payments'
    )

    # create d_rates table
    create_d_rates_table = GCSToBigQueryOperator(
        task_id='create_d_rates_table',
        bucket=BUCKET,
        source_objects=['rates.csv'],
        destination_project_dataset_table=f'{MAIN_DATASET}.d_rates'
    )

     # create d_vendors table
    create_d_vendors_table = GCSToBigQueryOperator(
        task_id="create_d_vendors_table",
        bucket=BUCKET,
        source_objects=['vendors.csv'],
        destination_project_dataset_table=f"{MAIN_DATASET}.d_vendors"
    )

    # these tasks can be run simultaneously but if you chooose a rather small environment size, 
    # you can run them sequentially to avoid resource-related errors.

    create_staging_dataset >> create_main_dataset >> create_f_trips_table >> create_d_weather_table
    create_d_weather_table >> create_d_locations_table >> create_d_payments_table >> create_d_rates_table
    create_d_rates_table >> create_d_vendors_table

task_flow()
