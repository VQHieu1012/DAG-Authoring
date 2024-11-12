from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
from include.datasets import DATASET, DATASET_2, DATASET_3


# dataset = Dataset("../tmp/my_file.txt")

with DAG(
    dag_id="producer_practice",
    schedule='@daily',
    start_date=datetime(2024, 11, 11),
    catchup=False
) as dag:
    
    @task(outlets=[DATASET, DATASET_2, DATASET_3])
    def update_dataset():
        with open(DATASET, 'a') as f:
            f.write('Producer update!')
    update_dataset()