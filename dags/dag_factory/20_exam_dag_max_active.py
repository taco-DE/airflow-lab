import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from module.airflow.conf import AirflowConf

conf = AirflowConf()
with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    start_date=pendulum.datetime(2023, 7, 1, tz="Asia/Seoul"),
    schedule=None,
    render_template_as_native_obj=True,
    catchup=False,
    max_active_tasks=3,
    max_active_runs=10,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    doc_md=__doc__,
) as dag:
    temp_task = EmptyOperator(task_id="temp")

    _ = temp_task
