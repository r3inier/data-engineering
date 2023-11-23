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
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['country'] = res['location']['country']
    data['state'] = res['location']['state']
    dob_date = res['dob']['date']
    data['dob'] = dob_date[8:10] + '-' + dob_date[5:7] + '-' + dob_date[0:4]
    data['age'] = res['dob']['age']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time

    res = get_data()
    res = format_data(res)
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    producer.send('users_created', json.dumps(res).encode('utf-8'))

with DAG('user_automation',
        default_args=default_args,
        catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    ) 