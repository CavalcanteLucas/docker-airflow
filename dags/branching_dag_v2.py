from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

from datetime import datetime

default_args = {"start_date": datetime(2022, 12, 20)}


def _choose_task():
    choose_upper_task = False
    if choose_upper_task:
        return "a_upper_task_branching_v2"
    else:
        return "b_lower_task_branching_v2"


dag = DAG(
    "branching_v2",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
)

with DAG(
    "branching_v2", schedule_interval="@daily", default_args=default_args, catchup=False
) as dag:

    branch_task = BranchPythonOperator(
        task_id="branch_task_branching_v2",
        python_callable=_choose_task,
        do_xcom_push=False,
    )

    upper_task = DummyOperator(task_id="a_upper_task_branching_v2")

    lower_task = DummyOperator(task_id="b_lower_task_branching_v2")

    final_task = DummyOperator(
        task_id="final_task_branching_v2", trigger_rule="none_failed_or_skipped"
    )

    branch_task >> [upper_task, lower_task] >> final_task
