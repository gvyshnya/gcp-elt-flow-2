"""
DAG script for daily export of joined ISIN, LEI, LegalName
to Azure storage, with slack notifications provided
Scheduled daily at 6:00 AM UTC 

    Functions
- save_all_csv
    extract data into csv files from GCS to workflow, 
    grouped by 'LegalAddress_Country'
- upload_by_country
    upload data file by file to azure blob 'isinreservoir'
- py_azure_upload
    remove files frome airflow filesystem
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

DAG_NAME = "ISIN_Export_by_country"
DAG_VERSION = "0.1"

PROJECT_NAME = os.environ.get("PROJECT_NAME")
SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT")
GCP_CONN_ID = os.environ.get("CONNECTION_ID")
BASE_BUCKET = os.environ.get("COMPOSER_BUCKET")
SLACK_CONN_ID = 'slack_isin_export'
WASB_CONN_ID = 'wasb_isin_export'

DATA_EXCHANGE_BASE_PATH = "/home/airflow/gcs/data/isin"
UPLOAD_DATE = datetime.date.today().strftime("%Y%m%d")
BY_COUNTRY_FOLDER = 'leicountry{}'.format(UPLOAD_DATE)
BY_COUNTRY_PATH = '/'.join([DATA_EXCHANGE_BASE_PATH, BY_COUNTRY_FOLDER])
#FILENAME = 'ISIN_export_{date}.csv'.format(date=UPLOAD_DATE)
#FILE_PATH = '/'.join([DATA_EXCHANGE_BASE_PATH, FILENAME])

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
        schedule_interval=None, # "6 0 * * *", # running the job daily, at 6 am UTC;
        on_failure_callback=commons.on_failure_callback,
        on_success_callback=commons.on_success_callback
) as dag:
    
    def save_lei_by_country(**kwargs):
        # select distinct countries
        # for each country id if there are records with it: 
        # select ISIN, LEI from Isins table
        # select LegalName from Entities table
        # inner join on LEI using BigQuery
        # export to csv and save in airflow local filesystem
        
        try:
            os.mkdir(BY_COUNTRY_PATH)
        except:
            print('directory {} already exists'.format(BY_COUNTRY_PATH))
        
        query_countries = f"""
            SELECT DISTINCT
            'LegalAddress_Country' as country_id
            FROM `{GCP_PROJECT_ID}.lei_golden.Entities`
            """

        df_countries = pd.read_gbq(
            query_countries,
            project_id=GCP_PROJECT_ID,
            dialect='standard',
        )
        
        for country_code in df_countries['country_id']:
            
            query = f"""
                SELECT 
                i.ISIN,
                i.LEI,
                e.LegalName,
                e.LegalAddress_Country as country_id
                FROM `{GCP_PROJECT_ID}.lei_golden.Entities` e 
                JOIN `{GCP_PROJECT_ID}.lei_golden.Isins` i
                ON e.LEI = i.LEI
                WHERE 
                e.LegalAddress_Country = '{country_code}';
                """

            df = pd.read_gbq(
                query,
                project_id=GCP_PROJECT_ID,
                dialect='standard'
            )
            
            if len(df):
                filename = 'LEI_{id}.csv'.format(id=country_code)
                file_path = '/'.join([BY_COUNTRY_PATH, filename])
                df.to_csv(file_path, index=False) 


    def upload_by_country(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task
        files = os.listdir(BY_COUNTRY_PATH)
        csv_files = [f for f in files if f.endswith('.csv')]

        for filename in csv_files:
            dynamic_path = '/'.join([BY_COUNTRY_PATH, filename])
            
            azure_upload = FileToWasbOperator(
                task_id='upload-to-azure',
                file_path=dynamic_path, 
                container_name=AZURE_CONTAINER, 
                blob_name=filename, 
                wasb_conn_id=WASB_CONN_ID
            )
            azure_upload.execute(context)


    def delete_isin(**kwargs):
        # deleting csv files from airflow directory BY_COUNTRY_PATH
        for filename in os.listdir(BY_COUNTRY_PATH):
            try:
                if filename.endswith('.csv'):
                    os.remove('/'.join([BY_COUNTRY_PATH, filename]))
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (filename, e))

    
    start = dummy_operator.DummyOperator(task_id="START")

    end = dummy_operator.DummyOperator(task_id="END")

    save_all_csv = PythonOperator(
        task_id="save-csv",
        python_callable=save_lei_by_country,
        provide_context=True
    )
    
    py_azure_upload = PythonOperator(
        task_id='delete-all-csv',
        python_callable=upload_by_country,
        provide_context=True
    )
    
    delete_all_csv = PythonOperator(
        task_id='delete-csv',
        python_callable=delete_isin,
        provide_context=True
    )

    start >> save_all_csv >> py_azure_upload >> delete_all_csv >> end
