from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from groups.process_tasks import process_tasks
from airflow.operators.dummy import DummyOperator

partners = {
    "partner_snowflake":{
        "name": "snowflake",
        "path": "/partners/snowflake"
    },
    "partner_netflix": {
        "name": "netflix",
        "path": "/partners/netflix"    
    }, 
    "partner_astronomer": {
        "name": "astronomer",
        "path": "/partners/astronomer"    
    }
}

default_args = {
    "start_date": datetime(2024, 11, 4),
}
@dag(description="DAG subdag demostration",
     default_args=default_args,
     schedule_interval='@daily',
     dagrun_timeout=timedelta(minutes=10), tags=["subdag"],
     catchup=False, max_active_runs=1)
def subdag_demo():
    start = DummyOperator(
        task_id="start"
    )
    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}", multiple_outputs=True)
        def extract(partner_name, partner_path):
            return {"partner_name": partner_name, "partner_path": partner_path}
        extracted_values = extract(details["name"], details["path"])
        start >> extracted_values
        process_tasks(extracted_values)
       

    
dag = subdag_demo()