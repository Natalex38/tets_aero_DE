documentation = """
# Тестовое Задание
"""
from datetime import timedelta

import pandas as pd
import pendulum
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine


DEFAULT_TASK_ARGS: dict = {
    "owner": "beaver",
    "retries": 3,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=3),
}

TARGET_TABLE_NAME: str = "test_aero"

POSTGRES_CONFIG: dict = {
    "username": "user",
    "password": "user_pass",
    "hostname": "172.18.0.2",
    "port": 5432,
    "database": "test_bd",
}

PANDAS_ENGINE_SQL_: str = create_engine(
    f"postgresql://{POSTGRES_CONFIG['username']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['hostname']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
)

def writing_data(data: pd.DataFrame) -> None:
    data.to_sql(name=TARGET_TABLE_NAME, con=PANDAS_ENGINE_SQL_, if_exists="replace", index=False)


def run_pipeline() -> None:
    response = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=100')
    data = response.json()
    request_df = pd.DataFrame.from_dict(data)
    writing_data(data=request_df)


with DAG(
        dag_id="test_task",
        start_date=pendulum.yesterday("UTC"),
        default_args=DEFAULT_TASK_ARGS,
        schedule_interval=timedelta(hours=12),
        catchup=False,
        max_active_runs=1,
        tags=["data_pipeline", "postgresql"],
        doc_md=documentation,
) as dag:
    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )

if __name__ == "__main__":
    run_pipeline()