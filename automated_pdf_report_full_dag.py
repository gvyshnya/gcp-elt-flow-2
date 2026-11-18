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

DAG_NAME = "Full_PDF_Report_Parser"
DAG_VERSION = "2.0"

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
PDF_REPORT_TABLES_TABLE = 'ReportTables'

REGION = 'europe-west3'

IDENTIFY_PDF_TASK_PREFIX = 'identify_pdf'
DETECT_PAGES_TASK_PREFIX = 'detect_pages'
PARSE_PDF_TASK_PREFIX = 'parse_pdf'

REPORTING_YEAR_STR = "2020"

DEFAULT_ARGS = {
    "owner": "xyz-DIGITAL",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021, 10, 12),
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
        description="Automated PDF Report Parsing DAG (reads data for cashflow statements)",
        default_args=DEFAULT_ARGS,
        schedule_interval=None, #"6 0 * * *", # running the job daily, at 6 am UTC;
        #on_failure_callback=commons.on_failure_callback,
        #on_success_callback=commons.on_success_callback
) as dag:

    def cat_by_na(row):
        cat = 'v'
        na_rate = row['sum_na']/len(row)
        if na_rate >= 0.5:
            cat = 'l'
        if (na_rate < 0.5) and (na_rate > 0):
            cat = 'v'
        return cat


    def normalize_header(df):
        df['sum_na'] = df.isna().sum(axis=1)
        df['cat'] = df.apply(cat_by_na, axis=1)
        cat_thread = ''.join(df['cat'])
        header_index = []
        label_first = cat_thread.startswith('l')
        i = 0
        while label_first:
            header_index.append(i)
            label_first = cat_thread[i:].startswith('l')
            i+=1
    
        part_df = df.iloc[header_index,:].astype(str)

        fix_header = part_df.agg('_'.join, axis=0)
        fix_header = fix_header.apply(lambda s: s.replace('nan_', ''))
        fix_header = fix_header.apply(lambda s: s.replace('nan', ''))

        header_df = pd.DataFrame(fix_header).T
        res_df = pd.concat([header_df, df.drop(header_index, axis=0)])
        res_df = res_df.drop(['sum_na', 'cat'], axis=1)
        res_df.reset_index(drop=True, inplace=True)
        return res_df

    def pattern_normalize(df, pattern):
        df['sum_na'] = df.isna().sum(axis=1)
        df['cat'] = df.apply(cat_by_na, axis=1)
        cat_thread = ''.join(df['cat'])
        matches = [m for m in re.finditer(pattern, cat_thread)]
        # print(matches)
        res_df = df.copy()
        for m in matches:
            span_range = (list(range(m.span()[0], m.span()[1])))

            part_df = df.iloc[span_range,:]
    
            label_col = '0'
            label_col = df.columns[0]

            labels_index = list(part_df[part_df['cat']=='l'].index.values)
            value_index = list(part_df[part_df['cat']=='v'].index.values)[0]
            fix_label = ''
            for line in part_df[label_col]:
                if line and (line!='nan'):
                    fix_label += str(line)
                    fix_label += ' '
            fix_label = fix_label.strip()
        
            res_df = res_df.drop(labels_index)
            res_df.loc[value_index, label_col] = fix_label


        res_df = res_df.drop(['sum_na', 'cat'], axis=1)    
        res_df.reset_index(drop=True, inplace=True)
        return res_df


    def normalize_vertical(df, pattern):
        new_df_T = df.T
        new_df = pattern_normalize(new_df_T, pattern).T
        return new_df


    def reset_header(df):
        old_names = list(df.iloc[0,:])
        new_names = old_names[:1]
        year_regex = r"[1,2][0,9][0-9][0-9]"

        # clean year values
        # for tables with column indexes representing years
        for name in old_names[1:]:
            matched_year = re.search(year_regex , name) 
            if matched_year:
                new_names.append(matched_year.group(0))
            else:
                new_names.append(name)

        new_col_dict = dict(zip(df.columns, new_names))
        res_df = df.rename(columns = new_col_dict, inplace=False)
        res_df = res_df.drop([0])
        res_df.reset_index(drop=True, inplace=True)
        return res_df


    def normalize_table(raw_df: pd.DataFrame):
        df = normalize_header(raw_df)
        df = pattern_normalize(df, 'lvl')
        df = pattern_normalize(df, 'lv')
        df = normalize_vertical(df, 'lv')
        df = pattern_normalize(df, 'lv')
        df = reset_header(df)
        return df

    def extract_number(string) -> str:
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
    
        
    def detect_cashflow_pages_in_pdf(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task

        sql = f"""
        SELECT 
            a.attribute_id,
            a.attribute_name
        FROM `{GCP_PROJECT_ID}.{PDF_REPORTS_DATASET}.AttributeConfiguration` a
            JOIN `{GCP_PROJECT_ID}.{PDF_REPORTS_DATASET}.ModelsToAttributes` ma 
                ON ma.attribute_id = a.attribute_id
            JOIN `{GCP_PROJECT_ID}.{PDF_REPORTS_DATASET}.ModelsConfiguration` m
                ON ma.model_id = m.model_id
        WHERE m.model_name = 'basic cash flow'
        """

        search_labels_df = pd.read_gbq(
            sql,
            project_id=GCP_PROJECT_ID,
            dialect='standard',
        )

        search_labels = list(set(search_labels_df["attribute_name"]))

        # get the list of pdf reports to parse
        pdf_reports = ti.xcom_pull(task_ids='get_files')

        pdf_docs_with_detected_pages = []

        for doc in pdf_reports:
            for search_lable in search_labels:
                request_data = {}
                request_data['path'] = get_full_gcs_path(doc['name'])
                request_data['max_number_of_pages'] = "1" # look for the top most relevant  page
                request_data['search_phrase'] = search_lable

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
        
        # prepare the output dictionary
        # key: doc_id  (internal id of the document in the batch)
        # value: page dictionary, with elements as follows
        #        - id (internal id of the document in the batch)
        #        - path (path to the document)
        #        - page_with_table_no - string: comma-separated numbers of pages to extract from the pdf file
        res = {}

        doc_pages = {}
        current_doc_id = ''
        new_doc_id = ''

        for page in pdf_docs_with_detected_pages:
            current_doc_id = page.get('id')
            if current_doc_id != new_doc_id:
                res[current_doc_id] = page
                new_doc_id = current_doc_id
            else:
                doc_pages = res.get(current_doc_id)
                additional_page_id = page.get('page_with_table_no')
                doc_pages['page_with_table_no'] = '{},{}'.format(doc_pages['page_with_table_no'], additional_page_id)
                res[current_doc_id] = doc_pages
                doc_pages = {}
        
        for key, value in res.items():
            doc_pages = value['page_with_table_no']
            page_numbers = doc_pages.split(',')

            # remove duplicates
            page_numbers = list(set(page_numbers))

            # remove "-1", if found
            new_page_no_values = ''
            for number in page_numbers:
                if number != '-1':
                    if new_page_no_values == '':
                        new_page_no_values = number
                    else:
                        new_page_no_values = ",".join([new_page_no_values, number])
            value['page_with_table_no'] = new_page_no_values

            res[key] = value
        
        #debug
        print(res)

        return res

            
    def detect_income_statement_pages_in_pdf(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task

        sql = f"""
        SELECT 
            a.attribute_id,
            a.attribute_name
        FROM `{GCP_PROJECT_ID}.{PDF_REPORTS_DATASET}.AttributeConfiguration` a
            JOIN `{GCP_PROJECT_ID}.{PDF_REPORTS_DATASET}.ModelsToAttributes` ma 
                ON ma.attribute_id = a.attribute_id
            JOIN `{GCP_PROJECT_ID}.{PDF_REPORTS_DATASET}.ModelsConfiguration` m
                ON ma.model_id = m.model_id
        WHERE m.model_name = 'basic income statement'
        """

        search_labels_df = pd.read_gbq(
            sql,
            project_id=GCP_PROJECT_ID,
            dialect='standard',
        )

        search_labels = list(set(search_labels_df["attribute_name"]))

        # get the list of pdf reports to parse
        pdf_reports = ti.xcom_pull(task_ids='get_files')

        pdf_docs_with_detected_pages = []

        for doc in pdf_reports:
            for search_lable in search_labels:
                request_data = {}
                request_data['path'] = get_full_gcs_path(doc['name'])
                request_data['max_number_of_pages'] = "1" # look for the top most relevant  page
                request_data['search_phrase'] = search_lable

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
        
        # prepare the output dictionary
        # key: doc_id  (internal id of the document in the batch)
        # value: page dictionary, with elements as follows
        #        - id (internal id of the document in the batch)
        #        - path (path to the document)
        #        - page_with_table_no - string: comma-separated numbers of pages to extract from the pdf file
        res = {}

        doc_pages = {}
        current_doc_id = ''
        new_doc_id = ''

        for page in pdf_docs_with_detected_pages:
            current_doc_id = page.get('id')
            if current_doc_id != new_doc_id:
                res[current_doc_id] = page
                new_doc_id = current_doc_id
            else:
                doc_pages = res.get(current_doc_id)
                additional_page_id = page.get('page_with_table_no')
                doc_pages['page_with_table_no'] = '{},{}'.format(doc_pages['page_with_table_no'], additional_page_id)
                res[current_doc_id] = doc_pages
                doc_pages = {}
        
        for key, value in res.items():
            doc_pages = value['page_with_table_no']
            page_numbers = doc_pages.split(',')

            # remove duplicates
            page_numbers = list(set(page_numbers))

            # remove "-1", if found
            new_page_no_values = ''
            for number in page_numbers:
                if number != '-1':
                    if new_page_no_values == '':
                        new_page_no_values = number
                    else:
                        new_page_no_values = ",".join([new_page_no_values, number])
            value['page_with_table_no'] = new_page_no_values

            res[key] = value
        
        #debug
        print(res)

        return res

        
    def detect_balance_pages_in_pdf(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task

        sql = f"""
        SELECT 
            a.attribute_id,
            a.attribute_name
        FROM `{GCP_PROJECT_ID}.{PDF_REPORTS_DATASET}.AttributeConfiguration` a
            JOIN `{GCP_PROJECT_ID}.{PDF_REPORTS_DATASET}.ModelsToAttributes` ma 
                ON ma.attribute_id = a.attribute_id
            JOIN `{GCP_PROJECT_ID}.{PDF_REPORTS_DATASET}.ModelsConfiguration` m
                ON ma.model_id = m.model_id
        WHERE m.model_name = 'basic balance sheet'
        """

        search_labels_df = pd.read_gbq(
            sql,
            project_id=GCP_PROJECT_ID,
            dialect='standard',
        )

        search_labels = list(set(search_labels_df["attribute_name"]))

        # get the list of pdf reports to parse
        pdf_reports = ti.xcom_pull(task_ids='get_files')

        pdf_docs_with_detected_pages = []

        for doc in pdf_reports:
            for search_lable in search_labels:
                request_data = {}
                request_data['path'] = get_full_gcs_path(doc['name'])
                request_data['max_number_of_pages'] = "1" # look for the top most relevant  page
                request_data['search_phrase'] = search_lable

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
        
        # prepare the output dictionary
        # key: doc_id  (internal id of the document in the batch)
        # value: page dictionary, with elements as follows
        #        - id (internal id of the document in the batch)
        #        - path (path to the document)
        #        - page_with_table_no - string: comma-separated numbers of pages to extract from the pdf file
        res = {}

        doc_pages = {}
        current_doc_id = ''
        new_doc_id = ''

        for page in pdf_docs_with_detected_pages:
            current_doc_id = page.get('id')
            if current_doc_id != new_doc_id:
                res[current_doc_id] = page
                new_doc_id = current_doc_id
            else:
                doc_pages = res.get(current_doc_id)
                additional_page_id = page.get('page_with_table_no')
                doc_pages['page_with_table_no'] = '{},{}'.format(doc_pages['page_with_table_no'], additional_page_id)
                res[current_doc_id] = doc_pages
                doc_pages = {}
        
        for key, value in res.items():
            doc_pages = value['page_with_table_no']
            page_numbers = doc_pages.split(',')

            # remove duplicates
            page_numbers = list(set(page_numbers))

            # remove "-1", if found
            new_page_no_values = ''
            for number in page_numbers:
                if number != '-1':
                    if new_page_no_values == '':
                        new_page_no_values = number
                    else:
                        new_page_no_values = ",".join([new_page_no_values, number])
            value['page_with_table_no'] = new_page_no_values

            res[key] = value
        
        #debug
        print(res)

        return res
        
    def parse_pdf_files(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task

        # get the dictionary with the info of pdf reports to parse
        pdf_reports_cashflow = ti.xcom_pull(task_ids='detect_cashflow_pages')
        pdf_reports_income = ti.xcom_pull(task_ids='detect_income_statement_pages')
        pdf_reports_balance = ti.xcom_pull(task_ids='detect_balance_pages')
        # get the list of guids we generated for the pdf reports being processed
        pdf_guids = ti.xcom_pull(task_ids='identify_files')

        table_name = PDF_REPORT_ATTRIBUTES_TABLE
        table_id = f"{PDF_REPORTS_DATASET}.{table_name}"
        if_exists_mode = 'append'

        # parse IFRS Statements data
        statements_data = [
            {
                "statement_type": "cf",
                "statement_type_full_name": "Cashflow Statement",
                "pdf_reports": pdf_reports_cashflow
            },
            {
                "statement_type": "is",
                "statement_type_full_name": "Income Statement",
                "pdf_reports": pdf_reports_income
            },
            {
                "statement_type": "bs",
                "statement_type_full_name": "Balance Sheet",
                "pdf_reports": pdf_reports_balance
            }
        ]

        for st in statements_data:
            attributes_data = []

            statement_type = st["statement_type"]
            statement_type_full_name = st["statement_type_full_name"]
            pdf_reports = st["pdf_reports"]

            for _key, doc in pdf_reports.items():
                if doc['page_with_table_no'] != "-1":
                    request_data = {}
                    request_data['path'] = doc['path']
                    # add 1 since detect_pages_in_pdf returns a zero-based page number
                    page_numbers = doc['page_with_table_no'].split(",")
                    new_page_number_line = ""
                    for num in page_numbers:
                        new_num_str = str(int(num) + 1)
                        if new_page_number_line == "":
                            new_page_number_line = new_num_str
                        else:
                            new_page_number_line = ",".join([new_page_number_line, new_num_str])
                
                    request_data['page_with_table_no'] = new_page_number_line

                    parse_task_id = "{}_{}_{}".format(PARSE_PDF_TASK_PREFIX, statement_type, doc["id"])

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

                    # save the parsed table data (as JSON) into the database
                    doc_parsed_data = {
                        'report_id': report_guid,	
                        'entity_lei': report_lei,
                        'statement_type': statement_type_full_name,
                        'table_info': response_body
                    }

                    attributes_data.append(doc_parsed_data)

            write_df = pd.DataFrame(data=attributes_data)

            if_exists_mode = 'append'
            table_name = PDF_REPORT_TABLES_TABLE
            table_id = f"{PDF_REPORTS_DATASET}.{table_name}"

            write_df.to_gbq(
                table_id,
                project_id=GCP_PROJECT_ID,
                if_exists=if_exists_mode
            )

            for doc_parsed_content in attributes_data:
                # parse out individual attributes
                # cast the return values to JSON
                json_dict = json.loads(doc_parsed_content["table_info"])
                entity_lei = doc_parsed_content["entity_lei"]

                # save each pdf table we extracted from the annual reports
                # as a separate database table in pdf_reports staging database
                for i in range(0, len(json_dict)):
                    table_rows = json.loads(json_dict[i]["rows"])
                    table_data = table_rows["data"]
                    col_count = len(table_rows["columns"])
                    cols = []
                    for j in range(col_count):
                        col_name = "".join(['c', str(j)])
                        cols.append(col_name)

                    df = pd.DataFrame(columns=cols, data=table_data)
                    # df = normalize_table(df)
                    df['LEI'] = entity_lei

                    print(df.info())

                    if_exists_mode = "replace"
                    table_name = "{}_{}_{}_{}".format(entity_lei, REPORTING_YEAR_STR, statement_type, str(i))
                    table_id = f"{PDF_REPORTS_DATASET}.{table_name}"

                    df.to_gbq(
                        table_id,
                        project_id=GCP_PROJECT_ID,
                        if_exists=if_exists_mode
                    )
    

    def task_success_slack(**kwargs):
        ti = kwargs['ti']  # get a task instance
        context = ti.get_template_context()  # get a context of the running task
        new_reports = ti.xcom_pull(task_ids='get_files')

        reports_list_str = ''
        for report in new_reports:
            reports_list_str += "\n{}".format(report['name'])
        slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
        slack_msg = """ 
            *Summary*: Extracted cashflow, income statement, and balance sheet data successfully.
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
    
    detect_cashflow_pages = PythonOperator(
        task_id="detect_cashflow_pages",
        python_callable=detect_cashflow_pages_in_pdf,
        provide_context=True
    )

    detect_income_pages = PythonOperator(
        task_id="detect_income_statement_pages",
        python_callable=detect_income_statement_pages_in_pdf,
        provide_context=True
    )

    detect_balance_pages = PythonOperator(
        task_id="detect_balance_pages",
        python_callable=detect_balance_pages_in_pdf,
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

    before_parse = dummy_operator.DummyOperator(task_id="BEFORE_PARSE")

    end = dummy_operator.DummyOperator(task_id="END")

    start >> lookup_pdf >> move_pdf_to_processed >> identify_pdf
    identify_pdf >> [detect_cashflow_pages, detect_income_pages, detect_balance_pages] >> before_parse
    before_parse >> parse_pdf >> notify_slack >> end