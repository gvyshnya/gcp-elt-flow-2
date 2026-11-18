from os import environ
import datetime
import requests
from zipfile import ZipFile
from zipfile import is_zipfile
import io
import json
from pytz import UTC

from google.cloud import storage

from airflow import models
from airflow.operators import dummy_operator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from dependencies import commons
from dependencies.tables import ISIN_TABLES, ISIN_MASTER_TABLES

DAG_NAME = "ISIN_Full_Reload"
DAG_VERSION = "0.3"

PROJECT_NAME = environ.get("PROJECT_NAME")
SERVICE_ACCOUNT = environ.get("SERVICE_ACCOUNT")
GCP_CONN_ID = environ.get("CONNECTION_ID")

BASE_DOWNLOAD_BUCKET = 'isin-files'
ZIP_FILE_NAME = 'isin-lei.zip'
UNZIPPED_CSV_FILE_NAME = 'isin-lei.csv'
ISIN_MAPPING_URL ='https://isinmapping.gleif.org/api/v2/isin-lei'

# Dataproc-related settings
REGION = 'europe-west1'
PYSPARK_SCRIPT = 'gs://xyz-dataproc-assets/csv_to_bq/main.py'
TEMPLATE_ID = 'csv_to_bq'
TEMP_GCP_BUCKET = 'temp-dataproc-storage'  # it should be passed without the uri prefix
STAGING_TABLE = 'Isins'

# Airflow environments/path shortcuts

QUERIES_BASE_PATH = "/home/airflow/gcs/dags/dependencies/queries/"
QUERIES_RELATIVE_BASE_PATH = "dependencies/queries/"

DEFAULT_ARGS = {
    "owner": "xyz-DIGITAL",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021, 7, 23),
    "email": [
        "orb@xyz.com",
    ],
    "email_on_failure": False,  # True
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=2),
}

JINJA_TEMPLATE_PARAMS = {
    "ingest_ts": datetime.datetime.utcnow()
        .replace(microsecond=0, second=0, minute=0, tzinfo=UTC)
        .isoformat(),
    "project_name": PROJECT_NAME,
}

with models.DAG(
        dag_id="{dag_name}-v{dag_version}".format(
            dag_name=DAG_NAME, dag_version=DAG_VERSION
        ),
        description="Performs a full reload of ISIN data.",
        default_args=DEFAULT_ARGS,
        schedule_interval="0 13 * * *",  # running the job daily, at 1 pm UTC; None was originally specified
        on_failure_callback=commons.on_failure_callback,
        on_success_callback=commons.on_success_callback
) as dag:

    def download_latest_isin_file(**kwargs):
        ti = kwargs['ti']  # get a task instance
        # REM: such a trick is not well documented in the official Airflow documentation
        # Inherited from https://bcb.github.io/airflow/execute-context
        context = ti.get_template_context()  # get a context of the running task

        today = datetime.datetime.today()
        current_date_yyyymmdd = today.strftime('%Y%m%d')

        # Making a request to get the valid download url
        isin_mapping_url = ISIN_MAPPING_URL
        try:
            response = requests.request("GET", isin_mapping_url)
            response_dict = json.loads(response.text)
        except:
            print('failed to load request')
            response_dict = response.text
        # extract download url from the mapping response
        try:
            isin_download_url = response_dict['data'][0]['attributes']['downloadLink']
        except:
            print('No valid download url found')
            isin_download_url = ''

        subdir = current_date_yyyymmdd

        filename = ZIP_FILE_NAME

        bash_command = f"""
            curl {isin_download_url} | gsutil cp - gs://{BASE_DOWNLOAD_BUCKET}/{subdir}/{filename}
        """
        b_op = BashOperator(
            task_id=f"{subdir}-download-zip",
            bash_command=bash_command,
        )
        b_op.execute(context)


    def unzip(**kwargs):
        ti = kwargs['ti']  # get a task instance
        # REM: such a trick is not well documented in the official Airflow documentation
        # Inherited from https://bcb.github.io/airflow/execute-context
        context = ti.get_template_context()  # get a context of the running task

        bucketname = BASE_DOWNLOAD_BUCKET

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucketname)

        today = datetime.datetime.today()
        current_date_yyyymmdd = today.strftime('%Y%m%d')

        subdir = current_date_yyyymmdd
        zip_file_name = ZIP_FILE_NAME

        zipfilename_with_path = '{}/{}'.format(subdir, zip_file_name)

        destination_blob_pathname = zipfilename_with_path

        blob = bucket.blob(destination_blob_pathname)
        zipbytes = io.BytesIO(blob.download_as_string())

        if is_zipfile(zipbytes):
            with ZipFile(zipbytes, 'r') as myzip:
                for contentfilename in myzip.namelist():
                    contentfile = myzip.read(contentfilename)

                    # put the contentfile into the key-value pair in Airflow XCOM to work with from
                    # the csv import operator down the road
                    # Note: it is safe just because GLEIF zip files always contain just one content file
                    output_csv = UNZIPPED_CSV_FILE_NAME
                    content_output_path = "{}/{}".format(zipfilename_with_path, output_csv)

                    blob = bucket.blob(content_output_path)
                    blob.upload_from_string(contentfile)


    def import_csv_to_bq(**kwargs):
        ti = kwargs['ti']  # get a task instance
        # REM: such a trick is not well documented in the official Airflow documentation
        # Inherited from https://bcb.github.io/airflow/execute-context
        context = ti.get_template_context()  # get a context of the running task

        dataset_id = kwargs['dataset_name']
        gcp_project_id = kwargs['project_name']

        today = datetime.datetime.today()
        current_date_yyyymmdd = today.strftime('%Y%m%d')

        subdir = "{}/{}".format(current_date_yyyymmdd, ZIP_FILE_NAME)
        csv_file_name = UNZIPPED_CSV_FILE_NAME

        table_name = STAGING_TABLE

        csv_file_uri = "gs://{}/{}/{}".format(BASE_DOWNLOAD_BUCKET, subdir, csv_file_name)

        bash_command = f"""
            gcloud dataproc workflow-templates instantiate \
            {TEMPLATE_ID} --project {gcp_project_id} --region {REGION} \
            --parameters MAIN_PYTHON_FILE={PYSPARK_SCRIPT},CSV_FILE={csv_file_uri},GCP_PROJECT_ID={gcp_project_id},DATASET_NAME={dataset_id},TABLE_NAME={table_name},TEMP_GCP_BUCKET_NAME={TEMP_GCP_BUCKET}
        """
        b_op = BashOperator(
            task_id=f"{dataset_id}-{table_name}-import-csv",
            bash_command=bash_command,
        )
        b_op.execute(context)

    def clean_up_latest_isin_folder(**kwargs):
        ti = kwargs['ti']  # get a task instance
        # REM: such a trick is not well documented in the official Airflow documentation
        # Inherited from https://bcb.github.io/airflow/execute-context
        context = ti.get_template_context()  # get a context of the running task

        today = datetime.datetime.today()
        current_date_yyyymmdd = today.strftime('%Y%m%d')
        subdir = current_date_yyyymmdd

        bash_command = f"""
           gsutil -m rm gs://{BASE_DOWNLOAD_BUCKET}/{subdir}/** || echo 'No files found in the bucket folder'
        """

        b_op = BashOperator(
            task_id=f"{current_date_yyyymmdd}-clean-up",
            bash_command=bash_command,
        )
        b_op.execute(context)

    start = dummy_operator.DummyOperator(task_id="START")
    end = dummy_operator.DummyOperator(task_id="END")

    ingest_to_staging = PythonOperator(
        task_id="import-csv-to-staging-bq",
        python_callable=import_csv_to_bq,
        op_kwargs={
            'dataset_name': 'lei_staging',
            'project_name': PROJECT_NAME,
        },
        provide_context=True
    )

    unzip = PythonOperator(
        task_id="unzip-csv",
        python_callable=unzip,
        provide_context=True
    )

    download = PythonOperator(
        task_id="download-zip",
        python_callable=download_latest_isin_file,
        provide_context=True
    )

    clean = PythonOperator(
        task_id="clean-up",
        python_callable=clean_up_latest_isin_folder,
        provide_context=True
    )

    staging_to_master = [
        commons.semantics_bigquery_job_merge_update(
            table_name=tname,
            relative_base_path=QUERIES_RELATIVE_BASE_PATH + "master",
            gcp_conn_id=GCP_CONN_ID,
            service_account=SERVICE_ACCOUNT,
            jinja_templating_params=JINJA_TEMPLATE_PARAMS,
            gcp_project=PROJECT_NAME,
        )
        for tname in ISIN_MASTER_TABLES
    ]

    start >> clean >> download >> unzip >> ingest_to_staging >> staging_to_master >> end
