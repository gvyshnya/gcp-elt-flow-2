"""
DAG script for daily export of joined ISIN, LEI, LegalName
to Azure storage, with slack notifications provided
Scheduled daily at 6:00 AM UTC 

    Functions
- save_csv
    extract data from the last day from GCS to workflow
- azure_upload
    upload data to azure blob 'isinreservoir'
- delete_csv
    remove file frome airflow filesystem
- on_success_callback 
    slack notification after successfull run
- on_failure_callback
    slack notification on failure
"""


import os
import pandas as pd
import datetime

from airflow import models
from airflow.operators import dummy_operator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.contrib.operators.file_to_wasb import FileToWasbOperator

from dependencies import commons

DAG_NAME = "ISIN_Export_by_date"
DAG_VERSION = "0.1"

PROJECT_NAME = os.environ.get("PROJECT_NAME")
SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT")
GCP_CONN_ID = os.environ.get("CONNECTION_ID")
BASE_BUCKET = os.environ.get("COMPOSER_BUCKET")
SLACK_CONN_ID = 'slack_isin_export'
WASB_CONN_ID = 'wasb_isin_export'

DATA_EXCHANGE_BASE_PATH = "/home/airflow/gcs/data/isin"
UPLOAD_DATE = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
FILENAME = 'ISIN_export_{date}.csv'.format(date=UPLOAD_DATE)
FILE_PATH = '/'.join([DATA_EXCHANGE_BASE_PATH, FILENAME])

AZURE_CONNSTR_PATH = '/home/airflow/gcs/data/'
AZURE_CONTAINER = 'isinreservoir'

GCP_PROJECT_ID = 'lorelee-keep-312708'
REGION = 'europe-west3'

DEFAULT_ARGS = {
    "owner": "xyz-DIGITAL",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021, 7, 6),
    "email": [
        "orb@xyz.com",
    ],
    "email_on_failure": False,  # True
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=2),
}


with models.DAG(
        dag_id="{dag_name}-v{dag_version}".format(
            dag_name=DAG_NAME, dag_version=DAG_VERSION
        ),
        description="Export ISIN",
        default_args=DEFAULT_ARGS,
        schedule_interval="6 0 * * *", # running the job daily, at 6 am UTC;
        on_failure_callback=commons.on_failure_callback,
        on_success_callback=commons.on_success_callback
) as dag:
    
    def save_joined_lei_by_date(**kwargs):
        # for current day
        # select ISIN, LEI from Isins table
        # select LegalName from Entities table
        # inner join on LEI using BigQuery
        # export to csv and save in airflow local filesystem
        
        query = f"""
            SELECT 
            i.ISIN,
            i.LEI,
            e.LegalName
            FROM `{GCP_PROJECT_ID}.lei_golden.Isins` i
            JOIN `{GCP_PROJECT_ID}.lei_golden.Entities` e
            ON e.LEI = i.LEI
            WHERE 
            i.ingest_date 
            BETWEEN '{UPLOAD_DATE} 00:00:00' 
            AND '{UPLOAD_DATE} 23:59:59';
            """

        df = pd.read_gbq(
            query,
            project_id=GCP_PROJECT_ID,
            dialect='standard'
        )

        df.to_csv(FILE_PATH, index=False) 


    def delete_isin(**kwargs):
        # deleting csv file from airflow file system
        os.remove(FILE_PATH)
    
    start = dummy_operator.DummyOperator(task_id="START")

    end = dummy_operator.DummyOperator(task_id="END")

    save_csv_by_date = PythonOperator(
        task_id="export-isin-by-date",
        python_callable=save_joined_lei_by_date,
        provide_context=True
    )
 
    azure_upload = FileToWasbOperator(
        task_id='upload-to-azure',
        file_path=FILE_PATH, 
        container_name=AZURE_CONTAINER, 
        blob_name=FILENAME, 
        wasb_conn_id=WASB_CONN_ID
    )
    
    delete_csv = PythonOperator(
        task_id='delete-csv',
        python_callable=delete_isin,
        provide_context=True
    )

    start >> save_csv_by_date >> azure_upload >> delete_csv >> end
