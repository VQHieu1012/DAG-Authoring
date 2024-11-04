from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from groups.process_tasks import process_tasks

@task.python(task_id="extract_partners", multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}

default_args = {
    "start_date": datetime(2024, 11, 4),
}
@dag(description="DAG subdag demostration",
     default_args=default_args,
     schedule_interval='@daily',
     dagrun_timeout=timedelta(minutes=10), tags=["subdag"],
     catchup=False, max_active_runs=1)
def subdag_demo():
    
    partner_settings = extract()
    process_tasks(partner_settings)
dag = subdag_demo()