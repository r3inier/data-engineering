from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'reinier',
    'start_date': datetime(2023, 10, 22, 10, 00)
}

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()['results'][0]

    return res

def format_data(res):
    data = {}

def stream_data():
    import json


# with DAG('user_automation',
#         default_args=default_args,
#         catchup=False) as dag:

#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )

stream_data()