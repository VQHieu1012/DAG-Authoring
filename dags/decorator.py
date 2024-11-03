from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag

@task.python
def extract(name, ti=None):
    print(f"Task extract {name}!!!")
    partner_name = "Degurech"
    return {
        "name": name,
        "partner_name": partner_name
    } 
    # key="return_value", value=name
    #ti.xcom_push(key="partner_name", value=name)

@task.python
def process(data):
    # partner_name = ti.xcom_pull(key="partner_name", task_ids="extract")
    # data = ti.xcom_pull(task_ids="extract")
    print(f"Task process {data["name"]}")
    print(f"Task process {data["partner_name"]}")
    

@dag(   description="DAG in charge of processing customer data",
         start_date=datetime(2024, 11, 1), schedule_interval='@daily',
         dagrun_timeout=timedelta(minutes=12),
         tags=["data engineer", "customer data"],
         catchup=False, max_active_runs=1)
        # if a dag runs for more than x minutes, then it fails
        # if schedule_interval = 10 minutes, maybe dagrun_interval needs to > 10 minutes 
        # max_active_runs: number of concurrent dag can run
def decorator():
    process(extract())    
    
decorator()
    