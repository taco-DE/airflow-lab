from dataclasses import dataclass

import inspect
import os.path
from datetime import timedelta, datetime

import pendulum
from airflow import DAG
from pendulum.tz.timezone import Timezone


@dataclass
class ScheduleConfiguration:
    """
    schedule conf VO class
    """

    tz: Timezone = pendulum.timezone("Asia/Seoul")
    schedule: str = None
    start_date: datetime = datetime(2022, 12, 31)
    end_date: datetime = None
    catchup: bool = False

    def __post_init__(self):
        self.start_date = pendulum.instance(self.start_date, tz=self.tz)

        if self.end_date:
            self.end_date = pendulum.instance(self.end_date, tz=self.tz)


class DAGFactory:
    def __init__(
        self,
        schedule_conf: ScheduleConfiguration = ScheduleConfiguration(),
        task_args: dict = None,
        tags: list = None,
    ):
        self._schedule = schedule_conf.schedule
        self._start_date = schedule_conf.start_date
        self._end_date = schedule_conf.end_date
        self._catchup = schedule_conf.catchup

        self._task_args = self._build_defualt_args_for_task(task_args)

        self._tags = tags

        caller_info = inspect.stack()[1]
        dag_file_name = caller_info.filename
        self._dag_id = os.path.basename(dag_file_name).replace(".py", "")
        self._doc_md = caller_info.frame.f_globals.get("__doc__")

        self._max_active_runs = 3
        self._max_active_tasks = 3
        self._description = None
        self._dagrun_timeout = None
        self._on_success_callback = None
        self._on_failure_callback =


    def _build_defualt_args_for_task(self, task_args) -> dict:
        args = {
            "retries": 0,
            "retry_delay": timedelta(minute=1),
        }
        if task_args:
            args.update(task_args)

        return args

    def description(self, desc: str):
        self._description = desc

        return self

    def max_active_runs(self, run_cnt: int):
        self._max_active_runs

        return self

    def dagrun_timeout(self, time: timedelta):
        self._dagrun_timeout = time

        return self

    def activate_success_callback(self):
        self._on_success_callback =

        return self

    def build_dag(self) -> DAG:
        return DAG(
            dag_id=self._dag_id,
            description=self._description,
            schedule=self._schedule,
            start_date=self._start_date,
            end_date=self._end_date,
            catchup=self._catchup,
            default_args=self._task_args,
            max_active_tasks=self._max_active_tasks,
            max_active_runs=self._max_active_runs,
            dagrun_timeout=self._dagrun_timeout,
            on_success_callback=self._on_success_callback,
            on_failure_callback=self.on_failure_callback,
            doc_md=self._doc_md,
            render_template_as_native_obj=self._render_template_as_native_obj,
            tags=self._tags,
            owner_links=self._owner_links,
        )
