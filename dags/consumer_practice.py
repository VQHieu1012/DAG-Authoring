from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime, timedelta
from include.datasets import DATASET, DATASET_2, DATASET_3

# dataset = Dataset("../tmp/my_file.txt")

with DAG(
    dag_id='consumer_practice',
    schedule=(DATASET & DATASET_2 | DATASET_3),
    start_date=datetime(2024, 11, 11),
    catchup=False
) as dag:
    
    @task
    def read_dataset():
        with open(DATASET.uri, 'r') as f:
            print(f.read())
            
    read_dataset()