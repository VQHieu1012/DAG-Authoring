from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime, timedelta

dataset = Dataset("../note.txt")

with DAG(
    dag_id="dataset_consumer",
    schedule=[dataset],
    start_date=datetime(2024, 11, 11),
    catchup=False
) as dag:
    
    @task()
    def read_dataset(triggering_dataset_events=None):
        print(triggering_dataset_events)
        with open(dataset.uri, 'r') as f:
            print(f.read())
    
    read_dataset()

