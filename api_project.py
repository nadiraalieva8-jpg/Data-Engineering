from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def stream_data():
    try:
        # 1. Fetch the data from the API
        response = requests.get('https://randomuser.me/api/', timeout=10)
        response.raise_for_status() # Raises an error if the site is down
        
        # 2. Extract JSON payload data
        data = response.json()
        user_results = data['results'][0] # Grab the first user object array
        
        # 3. Print cleanly structured data into your Airflow logs
        print(json.dumps(user_results, indent=3))
        
    except Exception as e:
        print(f"An error occurred fetching API data: {e}")
        raise e # Tells Airflow the task failed cleanly

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
