from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
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

def _choosing_partner_based_on_day(execution_date):
    day = execution_date.day_of_week
    if (day==1):
        return "extract_partner_snowflake"
    elif ( day == 3 ):
        return "extract_partner_netflix"
    elif ( day == 5 ):
        return "extract_partner_astronomer"
    return "stop"

default_args = {
    "start_date": datetime(2024, 11, 4),
}
@dag(description="DAG subdag demostration",
     default_args=default_args,
     schedule_interval='@daily',
     dagrun_timeout=timedelta(minutes=10), tags=["subdag"],
     catchup=False, max_active_runs=1)
def subdag_demo():
    
    choosing_partner_based_on_day = BranchPythonOperator(
        task_id="choosing_partner_based_on_day",
        python_callable=_choosing_partner_based_on_day
    )
    
    start = DummyOperator( task_id="start" )
    
    stop = DummyOperator( task_id = "stop")
    
    storing = DummyOperator( task_id = "storing", trigger_rule = 'none_failed_or_skipped' )
    
    choosing_partner_based_on_day >> stop
    
    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}", multiple_outputs=True)
        def extract(partner_name, partner_path):
            return {"partner_name": partner_name, "partner_path": partner_path}
        extracted_values = extract(details["name"], details["path"])
        start >> choosing_partner_based_on_day >> extracted_values
        process_tasks(extracted_values) >> storing
       

    
dag = subdag_demo()