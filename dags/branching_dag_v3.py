from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import chain

from datetime import datetime

default_args = {"start_date": datetime(2022, 12, 20)}


def _choose_task_1():
    choose_happy_path = True
    if choose_happy_path:
        return "AFTERPAY_afterpay_settlements_3_load_3"
    else:
        return "reconciliation_head_task"


def _choose_task_2():
    choose_happy_path = False
    if choose_happy_path:
        return "AFTERPAY_afterpay_settlements_currency_3_load_3"
    else:
        return "reconciliation_head_task"


dag = DAG(
    "branching_v3",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
)

web_scraper_tasks = []
split_csv_to_gcs_tasks = []
load_to_bigquery_tasks = []

web_scraper_tasks.append(
    DummyOperator(
        task_id="AFTERPAY_afterpay_settlements_currency_3_import_3",
        dag=dag,
    )
)


split_csv_to_gcs_tasks.append(
    BranchPythonOperator(
        task_id="AFTERPAY_afterpay_settlements_3_clean_3",
        python_callable=_choose_task_1,
        do_xcom_push=False,
        dag=dag,
    )
)


split_csv_to_gcs_tasks.append(
    BranchPythonOperator(
        task_id="AFTERPAY_afterpay_settlements_currency_3_dummy_clean_3",
        python_callable=_choose_task_2,
        do_xcom_push=False,
        dag=dag,
    )
)

load_to_bigquery_tasks.append(
    DummyOperator(
        task_id="AFTERPAY_afterpay_settlements_3_load_3",
        dag=dag,
    )
)


load_to_bigquery_tasks.append(
    DummyOperator(
        task_id="AFTERPAY_afterpay_settlements_currency_3_load_3",
        dag=dag,
    )
)

dbt_task = DummyOperator(
    task_id="AFTERPAY_PAYMENTS_DBT_3",
    trigger_rule="all_done",
    dag=dag,
)

reconciliation_head_task = DummyOperator(
    task_id="reconciliation_head_task",
    dag=dag,
)


web_scraper_tasks >> split_csv_to_gcs_tasks[0] >> [load_to_bigquery_tasks[0], reconciliation_head_task]
load_to_bigquery_tasks[0] >> dbt_task >> reconciliation_head_task

web_scraper_tasks >> split_csv_to_gcs_tasks[1] >> [load_to_bigquery_tasks[1], reconciliation_head_task]
load_to_bigquery_tasks[1] >> dbt_task >> reconciliation_head_task
