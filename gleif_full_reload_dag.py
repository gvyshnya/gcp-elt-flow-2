from os import environ
import datetime
from itertools import chain
import requests
import json
import objectpath

from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile
import io

from pytz import UTC
from airflow import models
from airflow.operators import dummy_operator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from dependencies import commons
from dependencies.tables import TABLES, MASTER_TABLES

DAG_NAME = "GLEIF_Full_Reload"
DAG_VERSION = "0.1"

PROJECT_NAME = environ.get("PROJECT_NAME")
SERVICE_ACCOUNT = environ.get("SERVICE_ACCOUNT")
GCP_CONN_ID = environ.get("CONNECTION_ID")

# Dataproc-related settings
REGION = 'europe-west1'
PYSPARK_SCRIPT = 'gs://xyz-dataproc-assets/csv_to_bq/main.py'
TEMPLATE_ID = 'csv_to_bq'
TEMP_GCP_BUCKET = 'temp-dataproc-storage'  # it should be passed without the uri prefix

BASE_DOWNLOAD_BUCKET = 'lei-data'
GLEIF_URL = "https://leidata-preview.gleif.org/api/v2/golden-copies/publishes?page=[object%20Object]&per_page=1"

# Airflow environments/path shortcuts

QUERIES_BASE_PATH = "/home/airflow/gcs/dags/dependencies/queries/"
QUERIES_RELATIVE_BASE_PATH = "dependencies/queries/"

DEFAULT_ARGS = {
    "owner": "xyz-DIGITAL",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021, 5, 16),
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
        description="Performs a full reload of GLEIF data.",
        default_args=DEFAULT_ARGS,
        schedule_interval="6 0 * * *",  # running the job daily, at 6 am UTC; None was originally specified
        on_failure_callback=commons.on_failure_callback,
        on_success_callback=commons.on_success_callback
) as dag:
    def download_files(**kwargs):
        ti = kwargs['ti']  # get a task instance
        # REM: such a trick is not well documented in the official Airflow documentation
        # Inherited from https://bcb.github.io/airflow/execute-context
        context = ti.get_template_context()  # get a context of the running task

        response = requests.request("GET", GLEIF_URL)

        data = json.loads(response.text)

        tree_obj = objectpath.Tree(data)

        full_file_nodes = tree_obj.execute('$..full_file')

        for node in full_file_nodes:
            node_obj = objectpath.Tree(node)
            csv_file_info = node_obj.execute('$.csv')

            url = csv_file_info['url']
            subdir = csv_file_info['type']

            filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names

            # save the file type (subdir) and the zip filename key-value pair in Airflow XCOM to work with from
            # different operators
            context['ti'].xcom_push(key=subdir, value=filename)

            bash_command = f"""
                curl {url} | gsutil cp - gs://{BASE_DOWNLOAD_BUCKET}/{subdir}/{filename}
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

        for tbl in TABLES:
            subdir = tbl['table_id']
            zip_file_name = context['ti'].xcom_pull(key=subdir)

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
                        content_output_path = "{}/{}".format(zipfilename_with_path, contentfilename)
                        context['ti'].xcom_push(key=subdir, value=content_output_path)

                        blob = bucket.blob(content_output_path)
                        blob.upload_from_string(contentfile)


    def import_csv_to_bq(**kwargs):
        ti = kwargs['ti']  # get a task instance
        # REM: such a trick is not well documented in the official Airflow documentation
        # Inherited from https://bcb.github.io/airflow/execute-context
        context = ti.get_template_context()  # get a context of the running task

        dataset_id = kwargs['dataset_name']
        gcp_project_id = kwargs['project_name']

        for tbl in TABLES:
            subdir = tbl['table_id']
            csv_file_name = context['ti'].xcom_pull(key=subdir)
            csv_file_uri = "gs://{}/{}".format(BASE_DOWNLOAD_BUCKET, csv_file_name)
            bash_command = f"""
                gcloud dataproc workflow-templates instantiate \
                {TEMPLATE_ID} --project {gcp_project_id} --region {REGION} \
                --parameters MAIN_PYTHON_FILE={PYSPARK_SCRIPT},CSV_FILE={csv_file_uri},GCP_PROJECT_ID={gcp_project_id},DATASET_NAME={dataset_id},TABLE_NAME={tbl['table_name']},TEMP_GCP_BUCKET_NAME={TEMP_GCP_BUCKET}
            """
            b_op = BashOperator(
                task_id=f"{dataset_id}-{tbl['table_name']}-import-csv",
                bash_command=bash_command,
            )
            b_op.execute(context)


    start = dummy_operator.DummyOperator(task_id="START")
    dummy_after_delete = dummy_operator.DummyOperator(task_id="DummyOp-GroupDeletes")
    dummy_after_staging = dummy_operator.DummyOperator(task_id="DummyOp-GroupStaging")
    end = dummy_operator.DummyOperator(task_id="END")

    delete_from_master = [
        commons.delete_table(tbl['table_name'], "lei_golden", gcp_project=PROJECT_NAME)
        for tbl in TABLES
    ]

    download = PythonOperator(
        task_id="download-csv",
        python_callable=download_files,
        provide_context=True
    )

    unzip = PythonOperator(
        task_id="unzip-csv",
        python_callable=unzip,
        provide_context=True
    )

    ingest_to_staging = PythonOperator(
        task_id="import-csv-to-staging-bq",
        python_callable=import_csv_to_bq,
        op_kwargs={
            'dataset_name': 'lei_staging',
            'project_name': PROJECT_NAME,
        },
        provide_context=True
    )

    staging_to_master = [
        commons.semantics_bigquery_job_full_reload(
            table_name=tname,
            relative_base_path=QUERIES_RELATIVE_BASE_PATH + "master",
            dataset="lei_golden",
            gcp_conn_id=GCP_CONN_ID,
            service_account=SERVICE_ACCOUNT,
            jinja_templating_params=JINJA_TEMPLATE_PARAMS,
            gcp_project=PROJECT_NAME,
        )
        for tname in MASTER_TABLES
    ]

    # this task will remove all of the files from the LEI download file output bucket
    # without deleting the bucket itself
    clean_up_bucket = BashOperator(
        task_id='cleanup-bucket',
        bash_command="gsutil -m rm gs://{}/** || echo 'No files found in the bucket'".format(BASE_DOWNLOAD_BUCKET)
    )

    start >> clean_up_bucket >> download >> unzip >> ingest_to_staging >> dummy_after_staging >> delete_from_master
    delete_from_master >> dummy_after_delete >> staging_to_master >> end
