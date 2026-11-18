from os import environ
import datetime

from google.cloud import storage

from pytz import UTC
from airflow import models

import pandas as pd
from bs4 import BeautifulSoup
import lxml

from airflow.operators import dummy_operator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from dependencies.esef import UNITS_MAPPING, CONTEXT_MAPPING

DAG_NAME = "ESEF_Parser"
DAG_VERSION = "0.1"

PROJECT_NAME = environ.get("PROJECT_NAME")
SERVICE_ACCOUNT = environ.get("SERVICE_ACCOUNT")
GCP_CONN_ID = environ.get("CONNECTION_ID")

BASE_DOWNLOAD_BUCKET = 'esef-reports'
BASE_LOGGING_BUCKET = 'etl-logging'

DEFAULT_DATASET = 'esef_staging'
ESEF_ATTRIBUTES_TABLE = 'EsefAttributes'
ESEF_DATA_TABLE = 'EsefReportsData'

# Airflow environments/path shortcuts

QUERIES_BASE_PATH = "/home/airflow/gcs/dags/dependencies/queries/"
QUERIES_RELATIVE_BASE_PATH = "dependencies/queries/"

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

TEMP_EESEF_UNZIPPED_BUCKET = 'esef-unzipped-temp'

ESEF_REPORTS = [
    {
        'company_id': 'AIRBUS',
        'report_path': 'data/airbus/ARB_ESEF-FINANCIAL_STATEMENTS_2020_EV_2021_03_25_ESEF.html',
        'unit_ref_attribute': 'unitref',
        'context_ref_attribute': 'contextref'
    },
    {
        'company_id': 'LUFTHANSA',
        'report_path': 'data/lufthansa/DeutscheLufthansa-2020-12-31.xhtml',
        'unit_ref_attribute': 'unitref',
        'context_ref_attribute': 'contextref'
    },
    {
        'company_id': 'STORA_ENSO',
        'report_path': 'data/stora_enso/7437000ZP669LKUTZ738-2021-02-10.xhtml',
        'unit_ref_attribute': 'unitref',
        'context_ref_attribute': 'contextref'
    },
    {
        'company_id': 'OUTOKUMPU',
        'report_path': 'data/outokumpu/20201231.xhtml',
        'unit_ref_attribute': 'unitref',
        'context_ref_attribute': 'contextref'
    },
    {
        'company_id': 'SANTANDER',
        'report_path': 'data/santander/5493006QMFDDMYWIAM13-20201231.xhtml',
        'unit_ref_attribute': 'unitref',
        'context_ref_attribute': 'contextref'
    },
    {
        'company_id': 'NORDEA',
        'report_path': 'data/nordea/NordeaBankAbp-2020-12-31.xhtml',
        'unit_ref_attribute': 'unitref',
        'context_ref_attribute': 'contextref'
    },
]

with models.DAG(
        dag_id="{dag_name}-v{dag_version}".format(
            dag_name=DAG_NAME, dag_version=DAG_VERSION
        ),
        description="Performs parsing the ESEF reports data.",
        default_args=DEFAULT_ARGS,
        schedule_interval=None,  # "6 0 * * *",  # running the job daily, at 6 am UTC; None was originally specified
        on_failure_callback=commons.on_failure_callback,
        on_success_callback=commons.on_success_callback
) as dag:
    def extract_timestamp_str(input_string: str) -> str:
        res = input_string

        date_prefixes = [
            'contexte_',
            '_I',
            '_D',
        ]

        for prefix in date_prefixes:
            if prefix in input_string:
                # extract the datetime stamp from the input string
                date_containg_str = input_string.partition(prefix)[2]
                if prefix == 'contexte_':
                    DD = date_containg_str[0:2]
                    MM = date_containg_str[2:4]
                    YYYY = date_containg_str[4:8]
                elif prefix == '_D' or prefix == '_I':
                    DD = date_containg_str[6:8]
                    MM = date_containg_str[4:6]
                    YYYY = date_containg_str[0:4]

                res = '{}-{}-{}'.format(YYYY, MM, DD)

        return res


    def get_esef_attributes(
            dataset: str,
            table_name: str,
    ):
        from airflow.contrib.operators import bigquery_get_data

        return bigquery_get_data.BigQueryGetDataOperator(
            dataset_id=dataset,
            table_id=table_name,
            max_results='1000',
            task_id="get_esef_attributes",
        )


    def parse_esef_token(
            contents: str,
            company_id: str,
            esef_token: str,
            unit_ref_attribute: str,
            context_ref_attribute: str
    ):
        records = []
        soup = BeautifulSoup(contents, "lxml")
        items = soup.find_all(attrs={"name": esef_token})
        if len(items) > 0:
            # print(items)
            i = 0
            for tag in items:
                i = i + 1

                record = {'company_id': company_id, 'attribute_name': tag.get('name'),
                          'units': tag.get(unit_ref_attribute),
                          'scale': tag.get('scale'), 'decimals': tag.get('decimals'), 'format': tag.get('format'),
                          'date_stamp': tag.get(context_ref_attribute)}
                if 'sign' in tag.attrs:
                    record['attribute_value'] = "".join(["-", tag.text])
                else:
                    record['attribute_value'] = tag.text

                records.append(record)
        return records


    def get_esef_staging_data(
            gcp_project_id: str,
    ) -> pd.DataFrame:
        query = """
            SELECT 
                company_id, 
                attribute_name,
                units,
                date_stamp,
                attribute_value
            FROM `{}.esef_staging.EsefReportsData`;
        """.format(gcp_project_id)

        df = pd.read_gbq(
            query,
            project_id=gcp_project_id,
            dialect='standard',
        )

        return df


    def esef_to_golden_replica(**kwargs):

        ti = kwargs['ti']  # get a task instance
        # REM: such a trick is not well documented in the official Airflow documentation
        # Inherited from https://bcb.github.io/airflow/execute-context
        context = ti.get_template_context()  # get a context of the running task

        esef_staging_df = get_esef_staging_data(PROJECT_NAME)

        # data clean-up/transformation
        # step 1: removing commmas and spaces from numeric values
        esef_staging_df['attribute_value'] = esef_staging_df['attribute_value'].str.replace(',', '')
        esef_staging_df['attribute_value'] = esef_staging_df['attribute_value'].str.replace(' ', '')

        # step 2: map units
        # Note: we use non-exhaustive mapping
        esef_staging_df['units'] = esef_staging_df['units'].map(UNITS_MAPPING).fillna(esef_staging_df['units'])

        # mapping datetime context stamps
        company_list = esef_staging_df['company_id'].unique()

        df_list = []

        for company in company_list:
            apply_date_stamp_transform_inline = True
            company_df = esef_staging_df[esef_staging_df['company_id'] == company]
            for ct in CONTEXT_MAPPING:
                company_id = ct.get('company_id')
                context_mapping_dict = ct.get('contexts')
                if company == company_id:
                    apply_date_stamp_transform_inline = False
                    company_df['date_stamp'] = company_df['date_stamp'].map(context_mapping_dict).fillna(
                        company_df['date_stamp'])
            if apply_date_stamp_transform_inline:
                # df['a'] = df['a'].apply(lambda x: x + 1)
                company_df['date_stamp'] = company_df['date_stamp'].apply(lambda x: extract_timestamp_str(x))

            df_list.append(company_df)

        esef_staging_df = pd.concat(df_list)

        # save the golden-copy data
        table_id = 'esef_golden.EsefReportsData'
        if_exists_mode = 'append'
        esef_staging_df.to_gbq(
            table_id,
            project_id=PROJECT_NAME,
            if_exists=if_exists_mode
        )


    def parse_esef_content(**kwargs):
        ti = kwargs['ti']  # get a task instance
        # REM: such a trick is not well documented in the official Airflow documentation
        # Inherited from https://bcb.github.io/airflow/execute-context
        context = ti.get_template_context()  # get a context of the running task

        bucketname = TEMP_EESEF_UNZIPPED_BUCKET  # BASE_DOWNLOAD_BUCKET

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucketname)

        # change to 'append' if needed
        if_exists_mode = 'replace'

        gcp_project_id = PROJECT_NAME
        dataset_id = 'esef_staging'
        table_name = ESEF_DATA_TABLE
        table_id = '{}.{}'.format(dataset_id, table_name)

        # Get ESEF attributes data from BQ in Python list
        task_id = "get_esef_attributes"
        attr_list = ti.xcom_pull(task_ids=task_id)

        i = 0

        for report in ESEF_REPORTS:
            records = []
            COMPANY_ID = report['company_id']
            print('Started processing: ', COMPANY_ID)
            esef_file_relative_uri = report['report_path']
            print("Report file uri: ", esef_file_relative_uri)

            if i > 0:
                if_exists_mode = 'append'

            unit_ref_attribute = report['unit_ref_attribute']
            context_ref_attribute = report['context_ref_attribute']

            blob = bucket.blob(esef_file_relative_uri)

            contents = blob.download_as_string()

            for esef_token_row in attr_list:
                esef_token = esef_token_row[0]
                tag_records = parse_esef_token(contents, COMPANY_ID, esef_token, unit_ref_attribute,
                                               context_ref_attribute)
                if len(tag_records) > 0:
                    for tr in tag_records:
                        records.append(tr)

            # cast to the dataframe
            df_esef = pd.DataFrame(records)
            df_esef = df_esef.drop_duplicates(keep='first')

            df_esef.to_gbq(
                table_id,
                project_id=gcp_project_id,
                if_exists=if_exists_mode
            )
            print('Finished processing: ', COMPANY_ID)
            i = i + 1


    sent_at = datetime.datetime.now()
    start = dummy_operator.DummyOperator(task_id="START")
    end = dummy_operator.DummyOperator(task_id="END")

    get_esef_attributes = get_esef_attributes(
        table_name=ESEF_ATTRIBUTES_TABLE,
        dataset=DEFAULT_DATASET,
    )

    parse_esef = PythonOperator(
        task_id="parse-esef",
        python_callable=parse_esef_content,
        provide_context=True
    )

    staging_to_golden = PythonOperator(
        task_id="esef-to-golden-copy",
        python_callable=esef_to_golden_replica,
        provide_context=True
    )

    start >> get_esef_attributes >> parse_esef >> staging_to_golden >> end
