from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime, timedelta

dataset = Dataset("../note.txt")

with DAG(
    dag_id="dataset_producer",
    schedule='@daily',
    start_date=datetime(2024, 11, 11),
    catchup=False
) as dag:
    
    @task(outlets=[dataset])
    def update_dataset():
        with open(dataset.uri, 'a') as f:
            f.write('## Update note!')
    
    update_dataset()

