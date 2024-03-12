from airflow.operators.empty import EmptyOperator

from module.airflow.conf import AirflowConf
from module.airflow.pattern.factory.dag_factory import DAGFactory, ScheduleConfiguration

conf = AirflowConf()
with (
    DAGFactory(schedule_conf=ScheduleConfiguration())
    .max_active_tasks(3)
    .max_active_runs(10)
    .build_dag() as dag
):
    temp_task = EmptyOperator(task_id="temp")

    _ = temp_task
