import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from module.airflow.conf import AirflowConf


def get_balance():
    logging.info(f">>> print get balance.")


def process_multi_thread() -> None:
    wallet_list = []

    logging.info(">>> Start process multi thread method.")

    with ThreadPoolExecutor(max_workers=3) as executer:
        wallet_info_futures = [executer.submit(get_balance) for _ in range(20)]
        # wallet_info_futures = [
        #     executer.submit(get_balance, symbol, wallet_type)
        #     for symbol, wallet_type in wallet_list
        # ]

        for future in as_completed(wallet_info_futures):
            logging.info(f"future: {future.result()}")


conf = AirflowConf()
with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    start_date=pendulum.datetime(2023, 7, 1, tz="Asia/Seoul"),
    schedule="0 * * * *",
    tags=["test", "thread"],
) as dag:
    test_multi_thread_task = PythonOperator(
        task_id="test_multi_thread",
        python_callable=process_multi_thread,
    )
