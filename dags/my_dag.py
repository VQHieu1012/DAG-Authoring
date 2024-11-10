from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def _extract(partner_name):
    # partner = Variable.get('my_dag_partner')
    # secret_partner = Variable.get('my_dag_secret')
    # partner_setting = Variable.get('my_dag_json', deserialize_json=True)
    # name = partner_setting['name']
    # passwd = partner_setting['api_secret']
    print(partner_name)
    
class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')
    
    
with DAG("my_dag", 
         description="DAG in charge of processing customer data",
         start_date=datetime(2024, 11, 1), schedule_interval='@daily',
         dagrun_timeout=timedelta(minutes=12),
         tags=["data engineer", "customer data"],
         catchup=False, max_active_runs=1) as dag: 
        # if a dag runs for more than x minutes, then it fails
        # if schedule_interval = 10 minutes, maybe dagrun_interval needs to > 10 minutes 
        # max_active_runs: number of concurrent dag can run

        extract = PythonOperator(
            task_id = 'extract',
            python_callable=_extract,
            op_args=["{{var.json.MY_DAG_JSON.name}}"]
        )
        
        fetching_data = CustomPostgresOperator(
            task_id="fetching_data",
            sql="./sql/MY_REQUEST.sql",
            parameters={
               'next_ds': '{{next_ds}}',
               'prev_ds': '{{prev_ds}}',
               'partner_name': '{{var.json.MY_DAG_JSON.name}}'
            }
        )
        
        trigger = TriggerDagRunOperator(
            task_id='trigger',
            trigger_dag_id="sensor_demo", # dag you want to trigger
            execution_date=datetime(2020, 2, 4), # use as execution date of the dag you want to trigger
            wait_for_completion=True, # wait for the "sensor demo" dag completes before execute the next task
            poke_interval=60, # check for the trigger dag completes or not
            reset_dag_run=True, # best practice
            failed_states=["failed", "skipped"]
        )
    