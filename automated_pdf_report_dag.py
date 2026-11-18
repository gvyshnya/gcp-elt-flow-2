# Note: the reason why xcom variables cannot be lists is explained by
#       https://stackoverflow.com/questions/47052582/pass-a-list-of-strings-as-parameter-of-a-dependant-task-in-airflow

# Key implementation ideas 
# 
# - initial list of pdf files as PythonOpeerator output
# - xcom_pull from multiple  operators on each step down the road (SimpleHTTPOperator to return the CF output)

import os
import datetime
import pandas as pd
import json
import logging
import uuid
import re

from google.cloud import storage

from airflow import models
from airflow.operators import dummy_operator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

from dependencies import commons

DAG_NAME = "PDF_Report_Parser"
DAG_VERSION = "1.3"

PROJECT_NAME = os.environ.get("PROJECT_NAME")
SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT")
GCP_CONN_ID = os.environ.get("CONNECTION_ID")
BASE_BUCKET = os.environ.get("COMPOSER_BUCKET")
CONTACT_SERVICE_CONN_ID = 'contact_cloud_function'
TABLE_PARSER_CONN_ID = 'parse_table_cf'
DETECT_PAGES_IN_PDF_CONN_ID = 'detect_pages_in_pdf_cf'
SLACK_CONN_ID = 'slack_isin_export'

DATA_EXCHANGE_BASE_PATH = "/home/airflow/gcs/data/"

CLIENT_BUCKET = 'itpdf-report-inbox'
BUCKET_NAME = 'xyz-pdf-reports'
DATA_FOLDER = 'data'
PROCESSED_FOLDER = 'processed'

GCP_PROJECT_ID = 'lorelee-keep-312708'
PDF_REPORTS_DATASET = 'pdf_reports'
PDF_REPORTS_TABLE = 'Reports'
PDF_REPORT_ATTRIBUTES_TABLE = 'ReportAttributes'

REGION = 'europe-west3'

IDENTIFY_PDF_TASK_PREFIX = 'identify_pdf'
DETECT_PAGES_TASK_PREFIX = 'detect_pages'
PARSE_PDF_TASK_PREFIX = 'parse_pdf'

SEARCH_LABEL = 'EBITDA'

DEFAULT_ARGS = {
    "owner": "xyz-DIGITAL",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021, 8, 31),
    "email": [
        "orb@xyz.com",
    ],
    "email_on_failure": False,  # True
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
}


with models.DAG(
        dag_id="{dag_name}-v{dag_version}".format(
            dag_name=DAG_NAME, dag_version=DAG_VERSION
        ),
        description="Automated PDF Report Parsing DAG",
        default_args=DEFAULT_ARGS,
        schedule_interval=None, #"6 0 * * *", # running the job daily, at 6 am UTC;
        on_failure_callback=commons.on_failure_callback,
        #on_success_callback=commons.on_success_callback
) as dag:

    def extract_number(string):
        result = re.sub(f"^[^0-9]+", "", string)
        result = re.sub(f"[^0-9]+$", "", result)
        return result

    def get_full_gcs_path(file_name: str) -> str:
        gcp_full_file_path = 'gs://{}/{}/{}'.format(BUCKET_NAME, PROCESSED_FOLDER, file_name)
        return gcp_full_file_path

    def find_new_reports(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task

        storage_client = storage.Client()
        client_blobs_pdf = storage_client.list_blobs(CLIENT_BUCKET)

        blobs_pdf = [b for b in client_blobs_pdf if b.name.endswith('.pdf')]

        logging.info("User's PDF blobs")
        pdf_names = []
        for blob in blobs_pdf:
            pdf_name = blob.name.split('/')[-1]
            pdf_names.append(pdf_name)
            logging.info(blob.name)

        # list to save new reports, non existent in database
        new_reports = [] 

        # fetch the info about already processed reports
        query = f"""
            SELECT report_name
            FROM `{GCP_PROJECT_ID}.{PDF_REPORTS_DATASET}.{PDF_REPORTS_TABLE}`
            """

        df = pd.read_gbq(
            query,
            project_id=GCP_PROJECT_ID,
            dialect='standard'
            )

        r_id = 1
        for file in pdf_names:
            if not file in list(df['report_name']):
                file_info = {
                    'id': '{:03d}'.format(r_id),
                    'name': file}
                new_reports.append(file_info)
                r_id += 1

        logging.info('New reports detected:')
        for report in new_reports:
            logging.info(report['name'])
        
        return new_reports
    
    def save_new_reports(**kwargs):
        # move newly uploaded reports from the user's bucket to the internal system bucket
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task

        new_reports = ti.xcom_pull(task_ids='get_files')

        # The ID of your GCS bucket
        bucket_name = CLIENT_BUCKET 
        # The ID of the bucket to move the object to
        destination_bucket_name = BUCKET_NAME

        storage_client = storage.Client()
        source_bucket = storage_client.bucket(bucket_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        for file in new_reports:
            # Relative path to user-uploaded report in the 'itpdf-report-inbox' bucket
            blob_name = file['name'] 
            # Relative path to save report in the 'xyz-pdf-reports' bucket
            destination_blob_name = '/'.join([PROCESSED_FOLDER, file['name']])
            # create blob instance for new report 
            source_blob = source_bucket.blob(blob_name)
            # move report to internal bucket 
            blob_copy = source_bucket.copy_blob(
                source_blob, destination_bucket, destination_blob_name
            )
            source_bucket.delete_blob(blob_name)

            print(
                "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
                    source_blob.name,
                    source_bucket.name,
                    blob_copy.name,
                    destination_bucket.name,
                )   
            )

    
    def identify_reports(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task

        table_name = 'Reports'
        table_id = f"{PDF_REPORTS_DATASET}.{table_name}"

        if_exists_mode = 'append'

        processing_timestamp = pd.Timestamp(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        report_guids = []

        # get the list of pdf reports to identify
        pdf_reports = ti.xcom_pull(task_ids='get_files')

        for doc in pdf_reports:
            request_data = {}
            request_data['path'] = get_full_gcs_path(doc['name'])

            identify_task_id = "{}_{}".format(IDENTIFY_PDF_TASK_PREFIX, doc["id"])

            trigger_operator = SimpleHttpOperator(
                task_id=identify_task_id,
                method='POST',
                http_conn_id=CONTACT_SERVICE_CONN_ID,
                endpoint='/',
                data=json.dumps(request_data, indent=4),  # my data
                headers={"Content-Type": "application/json"},
                xcom_push=True,
                response_check=(
                    lambda response: True if response.status_code == 200 else False
                )
            )

            response_body = trigger_operator.execute(context)
            # cast the return values to JSON
            json_dict = json.loads(response_body)

            # extract filename from path to file
            report_filename = json_dict['report'].split('/')[-1]

            report_guid_str = str(uuid.uuid4())

            report_guid = {}
            report_guid['report_guid'] = report_guid_str
            report_guid['report_id'] = doc['id']
            report_guid['LEI'] = json_dict['LEI']

            report_data_to_bq = {
                "report_id": report_guid_str, # random id as a GUID/UUID
                "report_name": report_filename,
                "ingest_date": processing_timestamp,
                "entity_lei": json_dict['LEI'],
                "entity_legal_name": json_dict['LegalName'],
                "entity_national_reg_code": json_dict['hrb'],
                "country_code": 'DE', # FIXME - return from the function properly
                "legal_address_city": json_dict['city']
            }

            df = pd.DataFrame(data=[report_data_to_bq])

            df.to_gbq(
                table_id,
                project_id=GCP_PROJECT_ID,
                if_exists=if_exists_mode
            )

            report_guids.append(report_guid)

        return report_guids
    
    def detect_pages_in_pdf(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task

        # get the list of pdf reports to parse
        pdf_reports = ti.xcom_pull(task_ids='get_files')

        pdf_docs_with_detected_pages = []

        for doc in pdf_reports:
            request_data = {}
            request_data['path'] = get_full_gcs_path(doc['name'])
            request_data['max_number_of_pages'] = "1" # look for the top most relevant  page
            request_data['search_phrase'] = SEARCH_LABEL

            detect_pages_task_id = "{}_{}".format(DETECT_PAGES_TASK_PREFIX, doc["id"])

            trigger_operator = SimpleHttpOperator(
                task_id=detect_pages_task_id,
                method='POST',
                http_conn_id=DETECT_PAGES_IN_PDF_CONN_ID,
                endpoint='/',
                data=json.dumps(request_data, indent=4),  # my data
                headers={"Content-Type": "application/json"},
                xcom_push=True,
                response_check=(
                    lambda response: True if response.status_code == 200 else False
                )
            )

            response_body = trigger_operator.execute(context)
            # cast the return values to JSON
            json_list = json.loads(response_body)

            if len(json_list) == 0:
                page_number_str = "-1" # just a fake page number
            else:
                page_number_str = json_list[0]

            report_info = {}
            report_info['id'] = doc['id']
            report_info['path'] = get_full_gcs_path(doc['name'])
            report_info['page_with_table_no'] = page_number_str

            pdf_docs_with_detected_pages.append(report_info)
        
        return pdf_docs_with_detected_pages


    def parse_pdf_files(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task

        # get the list of pdf reports to parse
        pdf_reports = ti.xcom_pull(task_ids='detect_pages')
        # get the list of guids we generated for the pdf reports being processed
        pdf_guids = ti.xcom_pull(task_ids='identify_files')

        # [report_id	entity_lei	attribute_name	attribute_year	attribute_value	]
        table_name = PDF_REPORT_ATTRIBUTES_TABLE
        table_id = f"{PDF_REPORTS_DATASET}.{table_name}"
        if_exists_mode = 'append'

        for doc in pdf_reports:
            if doc['page_with_table_no'] != "-1":
                request_data = {}
                request_data['path'] = doc['path']
                # add 1 since detect_pages_in_pdf returns a zero-based page number
                request_data['page_with_table_no'] = str(int(doc['page_with_table_no']) + 1) 

                parse_task_id = "{}_{}".format(PARSE_PDF_TASK_PREFIX, doc["id"])

                trigger_operator = SimpleHttpOperator(
                    task_id=parse_task_id,
                    method='POST',
                    http_conn_id=TABLE_PARSER_CONN_ID,
                    endpoint='/',
                    data=json.dumps(request_data, indent=4),  # my data
                    headers={"Content-Type": "application/json"},
                    xcom_push=True,
                    response_check=(
                        lambda response: True if response.status_code == 200 else False
                    )
                )

                response_body = trigger_operator.execute(context)
                # cast the return values to JSON
                json_dict = json.loads(response_body)

                # find the pdf report guid
                report_guid = 'Unknown'
                report_lei = 'Unknown'
                for guid in pdf_guids:
                    if guid['report_id'] == doc['id']:
                        report_guid = guid['report_guid']
                        report_lei = guid['LEI']

                df = pd.io.json.read_json(json_dict[0]['rows'])
                year_re = re.compile('[1,2][0,9][0-9][0-9](?![0-9])') # valid year
                ebitda_re = re.compile(SEARCH_LABEL) # starting with EBITDA
                # defaults for EBITDA allocation
                ebitda_records = []
                # itetrate over df to connect columns and years
                col_year = {} 
                for label, content in df.items():
                    for line in content:
                        res = year_re.match(line)
                        match_ebitda = ebitda_re.match(line)
                        if res:
                            col_year[label] = extract_number(line)    
                        if match_ebitda:
                            ebitda_dict = {
                                'col': label,
                                'alias': line
                            }
                            ebitda_records.append(ebitda_dict)

                attributes_data = []
                for ebitda in ebitda_records:
                    ebitda_serie = df[df[ebitda['col']]==ebitda['alias']]
                    for col in col_year:
                        attribute_info = {
                            'report_id' : report_guid,	
                            'entity_lei' : report_lei,
                            'attribute_name' : str(ebitda['alias']),	
                            'attribute_year' : str(col_year[col]),	
                            'attribute_value' : str(ebitda_serie[col].values[0])
                        }
                        attributes_data.append(attribute_info)

                write_df = pd.DataFrame(data=attributes_data)

                if_exists_mode = 'append'

                write_df.to_gbq(
                    table_id,
                    project_id=GCP_PROJECT_ID,
                    if_exists=if_exists_mode
                )
            else:
                print("No page with EBITDA in the tabular blocks detected; parsing skipped: ", doc['path'])

    def task_success_slack(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task
        new_reports = ti.xcom_pull(task_ids='get_files')

        reports_list_str = ''
        for report in new_reports:
            reports_list_str += "\n{}".format(report['name'])
        slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
        slack_msg = """ 
            *Task*: {task}  
            *Dag*: {dag} 
            *Reports moved*:{reports}
            *Execution Time*: {exec_date}  
            """.format(
            task="move-new-reports",
            dag=DAG_NAME,
            reports=reports_list_str,
            exec_date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        success_msg = SlackWebhookOperator(
            task_id='slack_notify',
            http_conn_id=SLACK_CONN_ID,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username='airflow')

        return success_msg.execute(context)
    
    notify_slack = PythonOperator(
        task_id="slack_notification",
        python_callable=task_success_slack,
        provide_context=True
    )

    parse_pdf = PythonOperator(
        task_id="parse_pdf",
        python_callable=parse_pdf_files,
        provide_context=True
    )

    detect_pages = PythonOperator(
        task_id="detect_pages",
        python_callable=detect_pages_in_pdf,
        provide_context=True
    )

    identify_pdf = PythonOperator(
        task_id="identify_files",
        python_callable=identify_reports,
        provide_context=True
    )

    move_pdf_to_processed = PythonOperator(
        task_id="move_files_to_processed",
        python_callable=save_new_reports,
        provide_context=True
    )

    lookup_pdf = PythonOperator(
        task_id="get_files",
        python_callable=find_new_reports,
        provide_context=True
    )

    start = dummy_operator.DummyOperator(task_id="START")

    end = dummy_operator.DummyOperator(task_id="END")

    start >> lookup_pdf >> move_pdf_to_processed >> identify_pdf >> detect_pages >> parse_pdf >> notify_slack
    notify_slack >> end