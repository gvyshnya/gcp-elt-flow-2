from os import environ
import datetime

import requests
from io import BytesIO

from google.cloud import storage

from pytz import UTC
from airflow import models
from airflow.operators import dummy_operator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

DAG_NAME = "ESEF_Archives_Downloader"
DAG_VERSION = "0.1"

PROJECT_NAME = environ.get("PROJECT_NAME")
SERVICE_ACCOUNT = environ.get("SERVICE_ACCOUNT")
GCP_CONN_ID = environ.get("CONNECTION_ID")

BASE_DOWNLOAD_BUCKET = 'esef-reports'
BASE_LOGGING_BUCKET = 'etl-logging'

TEMP_TABLE_NAME = '_files_to_download_'
DEFAULT_DATASET = 'esef_staging'

# Airflow environments/path shortcuts

QUERIES_BASE_PATH = "/home/airflow/gcs/dags/dependencies/queries/"
QUERIES_RELATIVE_BASE_PATH = "dependencies/queries/"

# misc constants
fake_lei = "fake-lei"

DEFAULT_ARGS = {
    "owner": "xyz-DIGITAL",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021, 6, 9),
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
        description="Performs the download of zipped ESEF reports data.",
        default_args=DEFAULT_ARGS,
        schedule_interval=None,  # "6 0 * * *",  # running the job daily, at 6 am UTC; None was originally specified
        on_failure_callback=commons.on_failure_callback,
        on_success_callback=commons.on_success_callback
) as dag:
    sent_at = datetime.datetime.now()


    def create_temp_table(
            table_name: str,
            dataset: str,
            gcp_project: str = PROJECT_NAME,
    ):
        from airflow.contrib.operators import bigquery_operator

        query = (
            "CREATE OR REPLACE TABLE `{project}.{dataset}.{table_name}` AS"
            " SELECT company_lei, esef_report_url, downloaded_file_uri"
            " FROM `{project}.{dataset}.EsefReports` "
            " WHERE "
            " downloaded_file_uri is NULL "
            " UNION ALL"
            " SELECT '{fake_lei}' as company_lei, 'fake-url' as esef_report_url, 'fake' as downloaded_file_uri "
        )

        query = query.format(
            fake_lei=fake_lei,
            project=gcp_project,
            dataset=dataset,
            table_name=table_name,
        )

        return bigquery_operator.BigQueryOperator(
            task_id="create-{}-{}".format(dataset, table_name),
            sql=query,
            location="EU",
            use_legacy_sql=False,
        )


    def update_download_status_temp_table(
            company_lei: str,
            downloaded_file_uri: str,
            table_name: str,
            dataset: str,
            gcp_project: str = PROJECT_NAME,
    ):
        from airflow.contrib.operators import bigquery_operator

        query = (
            "UPDATE `{project}.{dataset}.{table_name}`"
            " SET downloaded_file_uri = '{downloaded_file_uri}'"
            " WHERE company_lei = '{company_lei}' "
        )

        query = query.format(
            company_lei=company_lei,
            downloaded_file_uri=downloaded_file_uri,
            project=gcp_project,
            dataset=dataset,
            table_name=table_name,
        )

        return bigquery_operator.BigQueryOperator(
            task_id="update_download_status-temp-{}-{}-{}".format(dataset, table_name, company_lei),
            sql=query,
            location="EU",
            use_legacy_sql=False,
        )


    def update_download_status(
            table_name: str,
            dataset: str,
            gcp_project: str = PROJECT_NAME,
    ):
        from airflow.contrib.operators import bigquery_operator

        query = (
            "UPDATE `{project}.{dataset}.EsefReports` a"
            " SET a.downloaded_file_uri = b.downloaded_file_uri "
            " FROM `{project}.{dataset}.{table_name}` b "
            " WHERE a.company_lei = b.company_lei "
        )

        query = query.format(
            project=gcp_project,
            dataset=dataset,
            table_name=table_name,
        )

        return bigquery_operator.BigQueryOperator(
            task_id="update_download_status-{}-{}".format(dataset, table_name),
            sql=query,
            location="EU",
            use_legacy_sql=False,
        )


    def drop_temp_table(
            table_name: str,
            dataset: str,
            gcp_project: str = PROJECT_NAME,
    ):
        from airflow.contrib.operators import bigquery_operator

        query = (
            "DROP TABLE `{project}.{dataset}.{table_name}`"
        )

        query = query.format(
            project=gcp_project,
            dataset=dataset,
            table_name=table_name,
        )

        return bigquery_operator.BigQueryOperator(
            task_id="drop-{}-{}".format(dataset, table_name),
            sql=query,
            # bigquery_conn_id=gcp_conn_id,
            # delegate_to=service_account,
            location="EU",
            use_legacy_sql=False,
        )


    def get_data_from_temp_table(
            dataset: str,
            table_name: str,
    ):
        from airflow.contrib.operators import bigquery_get_data

        return bigquery_get_data.BigQueryGetDataOperator(
            dataset_id=dataset,
            table_id=table_name,
            max_results='1000',
            task_id="get_data_from_bq-{}".format(table_name),
        )


    def download_files(**kwargs):
        ti = kwargs['ti']  # get a task instance
        # REM: such a trick is not well documented in the official Airflow documentation
        # Inherited from https://bcb.github.io/airflow/execute-context
        context = ti.get_template_context()  # get a context of the running task

        bucketname = BASE_DOWNLOAD_BUCKET

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucketname)

        # Get BQ data in Python list
        task_id = "get_data_from_bq-{}".format(TEMP_TABLE_NAME)
        files_to_download = ti.xcom_pull(task_ids=task_id)

        for file in files_to_download:
            company_lei = file[0]
            file_uri = file[1]

            if company_lei != fake_lei:
                print("Company to process: ", company_lei)
                print("File to download: ", file_uri)

                # note: the reason to use stream=True explained in
                # https://stackoverflow.com/questions/16778435/python-check-if-website-exists
                try:
                    zip_file_name = file_uri.split("/")[-1]
                    with requests.get(file_uri, stream=True) as response:
                        try:

                            destination_blob_pathname = '{}/{}'.format(company_lei, zip_file_name)
                            downloaded_file_uri = 'gs://{}/{}'.format(bucketname, destination_blob_pathname)

                            blob = bucket.blob(destination_blob_pathname)
                            content_file = BytesIO(response.content).getvalue()
                            blob.upload_from_string(content_file)

                            bq_update_op = update_download_status_temp_table(
                                company_lei=company_lei,
                                downloaded_file_uri=downloaded_file_uri,
                                table_name=TEMP_TABLE_NAME,
                                dataset=DEFAULT_DATASET,
                                gcp_project=PROJECT_NAME,
                            )
                            bq_update_op.execute(context)

                        except requests.exceptions.HTTPError:
                            # TODO: log an error message return False
                            print("HTTP Error occurred")
                except requests.exceptions.ConnectionError:
                    # TODO: log an error message return False
                    print("Generic Connection Error occurred")


    start = dummy_operator.DummyOperator(task_id="START")
    end = dummy_operator.DummyOperator(task_id="END")

    create_tmp_table = create_temp_table(
        table_name=TEMP_TABLE_NAME,
        dataset=DEFAULT_DATASET,
        gcp_project=PROJECT_NAME,
    )

    get_file_list = get_data_from_temp_table(
        table_name=TEMP_TABLE_NAME,
        dataset=DEFAULT_DATASET,
    )

    download = PythonOperator(
        task_id="download-zip",
        python_callable=download_files,
        provide_context=True
    )

    update_download_status = update_download_status(
        table_name=TEMP_TABLE_NAME,
        dataset=DEFAULT_DATASET,
        gcp_project=PROJECT_NAME,
    )

    drop_tmp_tbl = drop_temp_table(
        table_name=TEMP_TABLE_NAME,
        dataset=DEFAULT_DATASET,
        gcp_project=PROJECT_NAME,
    )

    start >> create_tmp_table >> get_file_list >> download >> update_download_status >> drop_tmp_tbl >> end
