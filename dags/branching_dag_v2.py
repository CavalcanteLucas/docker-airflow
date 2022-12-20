from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import chain

from datetime import datetime

default_args = {"start_date": datetime(2022, 12, 20)}


def _choose_task():
    choose_upper_task = False
    if choose_upper_task:
        return "c_upper_task_branching_v2"
    else:
        return "d_lower_task_branching_v2"


dag = DAG(
    "branching_v2",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
)


branch_task = BranchPythonOperator(
    task_id="branch_task_branching_v2",
    python_callable=_choose_task,
    do_xcom_push=False,
    dag=dag,
)


upper_task = DummyOperator(
    task_id="c_upper_task_branching_v2",
    dag=dag,
)

lower_task = DummyOperator(
    task_id="d_lower_task_branching_v2",
    dag=dag,
)

final_task = DummyOperator(
    task_id="final_task_branching_v2",
    trigger_rule="none_failed_or_skipped",
    dag=dag,
)

chain(
    branch_task,
    [upper_task, lower_task],
    final_task,
)
