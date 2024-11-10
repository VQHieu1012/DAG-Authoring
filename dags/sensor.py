from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from groups.process_tasks import process_tasks
from airflow.sensors.date_time import DateTimeSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from time import sleep

partners = {
    "partner_snowflake":{
        "name": "snowflake",
        "path": "/partners/snowflake",
        "priority": 2,
        "pool": "snowflake"
    },
    "partner_netflix": {
        "name": "netflix",
        "path": "/partners/netflix",
        "priority": 3,
        "pool": "netflix"
    }, 
    "partner_astronomer": {
        "name": "astronomer",
        "priority": 1,
        "path": "/partners/astronomer",
        "pool": "astronomer"
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
     dagrun_timeout=timedelta(minutes=10),
     schedule_interval='@daily', tags=["subdag"],
     catchup=False, max_active_runs=1)
def subdag_demo():
    
    # choosing_partner_based_on_day = BranchPythonOperator(
    #     task_id="choosing_partner_based_on_day",
    #     python_callable=_choosing_partner_based_on_day
    # )
    
    start = DummyOperator( task_id="start" , execution_timeout=timedelta(minutes=5))
    
    # stop = DummyOperator( task_id = "stop")
    delay = DateTimeSensor(
        task_id="delay",
        target_time="{{execution_date.add(hours=9)}}",
        poke_interval=60 * 60 * 10
        # timeout + soft_fail -> when a task failed because of timeout, its state is skipped
        # execution_timeout=60 -> for every operator
        # exponential_backoff=True -> increase the waiting time between the interval of time,
        # or between each poke interval, ex: instead of waiting 60s, it may be 70s
    )
    
    waiting_for_task = ExternalTaskSensor(
        task_id='waiting_for_task',
        external_task_id='dummy',
        external_dag_id='my_dag'
    )
    
    storing = DummyOperator( task_id = "storing", trigger_rule = 'none_failed_or_skipped' )
    
    # choosing_partner_based_on_day >> stop
    # subdag: if we set pool to subdag, task in subdag will not respect this pool, only this subdag does
    
    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}", depends_on_past=True, pool_slots=1, priority_weight=details["priority"], pool=details['pool'], multiple_outputs=True)
        def extract(partner_name, partner_path):
            sleep(3)
            return {"partner_name": partner_name, "partner_path": partner_path}
        extracted_values = extract(details["name"], details["path"])
        # start >> choosing_partner_based_on_day >> extracted_values
        start >> waiting_for_task >> extracted_values
        process_tasks(extracted_values) >> storing
       

    
dag = subdag_demo()