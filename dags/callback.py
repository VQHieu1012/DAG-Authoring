from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSensorTimeout, AirflowTaskTimeout

def _extract(partner_name):
    # partner = Variable.get('my_dag_partner')
    # secret_partner = Variable.get('my_dag_secret')
    # partner_setting = Variable.get('my_dag_json', deserialize_json=True)
    # name = partner_setting['name']
    # passwd = partner_setting['api_secret']
    # raise KeyError
    print(partner_name)
    
class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')
    
def _success_callback(context):
    print(context)


def _failure_callback(context):
    print(context)
    
def _on_success_callback(context):
    print("SUCCESS CALLBACK!")

def _on_failure_callback(context):
    # what if my task failed because of timeout
    if context['exception']:
        if isinstance(context['exception'], AirflowTaskTimeout): 
            print("Task timeout")
        if isinstance(context['exception'], AirflowSensorTimeout): 
            print("Sensor timeout")
            
    print("FAILURE CALLBACK!")

def _on_retry_callback(context):
    if context['ti'].try_number() > 2:
        print("RETRY > 2")
    print("RETRY CALLBACK!")
    
with DAG("callback_dag", 
         description="DAG in charge of processing customer data",
         start_date=datetime(2024, 11, 1), schedule_interval='@daily',
         dagrun_timeout=timedelta(minutes=12),
         tags=["data engineer", "customer data"],
         catchup=False, max_active_runs=1, on_success_callback=_success_callback,
         on_failure_callback=_failure_callback) as dag: 
        # if a dag runs for more than x minutes, then it fails
        # if schedule_interval = 10 minutes, maybe dagrun_interval needs to > 10 minutes 
        # max_active_runs: number of concurrent dag can run

        extract = PythonOperator(
            task_id = 'extract',
            python_callable=_extract,
            
            op_args=["{{var.json.MY_DAG_JSON.name}}"],
            on_success_callback=_on_success_callback,
            on_failure_callback=_on_failure_callback,
            on_retry_callback=_on_retry_callback
        )
        
        fetching_data = CustomPostgresOperator(
            task_id="fetching_data",
            sql="./sql/MY_REQUEST.sql",
            parameters={
               'next_ds': '{{next_ds}}',
               'prev_ds': '{{prev_ds}}',
               'partner_name': '{{var.json.MY_DAG_JSON.name}}'
            },
            on_failure_callback=_on_failure_callback,
            retries=3,
            retry_delay=timedelta(minutes=0.5),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=5)
        )
    