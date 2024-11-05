from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import cross_downstream, chain_linear, chain
from datetime import datetime, timedelta

default_args = {
    "start_date": datetime(2024, 11, 5)
}

with DAG("dependency", default_args=default_args,
         schedule_interval='@daily', catchup=False) as dag:
    
    t1 = DummyOperator(task_id="t1")
    t2 = DummyOperator(task_id="t2")
    t3 = DummyOperator(task_id="t3")
    t4 = DummyOperator(task_id="t4")
    t5 = DummyOperator(task_id="t5")
    t6 = DummyOperator(task_id="t6")
    
    # [t1, t2, t3] >> [t4, t5]
    # cross_downstream([t1, t2, t3], [t4, t5, t6])
    # chain_linear([t1,t2,t3], [t4,t5])
    chain(t1, [t2, t3], [t4, t5], t6)
    