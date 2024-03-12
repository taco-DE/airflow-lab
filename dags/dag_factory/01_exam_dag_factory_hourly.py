from airflow.operators.empty import EmptyOperator

from module.airflow.conf import AirflowConf
from module.airflow.pattern.factory.dag_factory import DAGFactory, ScheduleConfiguration

conf = AirflowConf()
with DAGFactory(
    schedule_conf=ScheduleConfiguration(
        schedule="1 * * * *",
    ),
    tags=["test", "thread"],
).build_dag() as dag:
    temp_task = EmptyOperator(task_id="temp")

    _ = temp_task
