from os import environ
from typing import Dict, List
import datetime
from airflow.contrib.operators import bigquery_get_data

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

DEFAULT_PROJECT = "lorelee-keep-312708"
SLACK_CONN_ID = 'slack_isin_export'

def on_failure_callback(context):
    # slack notification after successful DAG run 
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='failure-notification',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)


def on_success_callback(context):
    # slack notification on failed DAG run 
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            DAG run successful.   
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            """.format(
            dag=context.get('task_instance').dag_id,
            exec_date=context.get('execution_date')
        )
    success_msg = SlackWebhookOperator(
            task_id='success-notification',
            http_conn_id=SLACK_CONN_ID,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username='airflow')

    return success_msg.execute(context=context)


def semantics_bigquery_job_merge_update(
    table_name: str,
    relative_base_path: str,
    gcp_conn_id: str,
    jinja_templating_params: Dict[str, str],
    base_query_dir: str = "./",
    service_account: str = None,
    gcp_project: str = DEFAULT_PROJECT,
):
    from airflow.contrib.operators import bigquery_operator
    from jinja2 import Template

    return bigquery_operator.BigQueryOperator(
        task_id="{}-MergeUpdate-toSemantics".format(table_name),
        sql="{rel_base_path}/{table}.sql".format(
            rel_base_path=relative_base_path, table=table_name
        ),
        allow_large_results=True,
        bigquery_conn_id=gcp_conn_id,
        delegate_to=service_account,
        location="EU",
        use_legacy_sql=False,
        params=jinja_templating_params,
    )

def semantics_bigquery_job_full_reload(
    table_name: str,
    relative_base_path: str,
    dataset: str,
    gcp_conn_id: str,
    jinja_templating_params: Dict[str, str],
    service_account: str = None,
    gcp_project: str = DEFAULT_PROJECT,
    truncate_table: bool = True,
):
    from airflow.contrib.operators import bigquery_operator

    return bigquery_operator.BigQueryOperator(
        task_id="{}-toSemantics".format(table_name),
        sql="{rel_base_path}/{table}.sql".format(
            rel_base_path=relative_base_path, table=table_name
        ),
        destination_dataset_table="{}.{}.{}".format(gcp_project, dataset, table_name),
        write_disposition="WRITE_TRUNCATE" if truncate_table else "WRITE_APPEND",
        allow_large_results=True,
        bigquery_conn_id=gcp_conn_id,
        delegate_to=service_account,
        location="EU",
        use_legacy_sql=False,
        params=jinja_templating_params,
    )


def delete_table(table_name: str, dataset: str, gcp_project: str = DEFAULT_PROJECT):
    from airflow.contrib.operators import bigquery_operator

    return bigquery_operator.BigQueryOperator(
        task_id="delete-{}".format(table_name.lower()),
        sql="delete FROM `{project}.{dataset}.{table}` where true".format(
            project=gcp_project, dataset=dataset, table=table_name
        ),
        location="EU",
        use_legacy_sql=False,
    )

