from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator


def say_hello(**context):
    import time

    print("before sleep")
    time.sleep(60)
    date = context["params"]["date"]
    print("Hello, world!", date)


with DAG(
    dag_id="hello_world",
    start_date=datetime.now(),
    schedule_interval="@daily",  # Run once per day
    catchup=False,  # Don't backfill missed runs
    tags=["example"],
    params={
        "date": Param("2024-04-20", type="string"),
    },
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
    )
