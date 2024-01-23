from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.utils.dates import days_ago
from time import time_ns
from datetime import datetime

default_args = {
    'owner': 'Clinica Internacional',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

def start_process():
    print(" INICIO EL PROCESO!")

def end_process():
    print(" FIN DEL PROCESO!")

def load():
    print(" Hola Airflow!")

with DAG(
    dag_id="mi_primer_dag", schedule="@once",
    start_date=days_ago(1), 
    default_args=default_args,
    description='Prueba de Dag'
) as dag:
    step_start = PythonOperator(
        task_id='step_start',
        python_callable=start_process,
        dag=dag
    )
    step_load = PythonOperator(
        task_id='step_load',
        python_callable=load,
        dag=dag
    )
    step_end = PythonOperator(
        task_id='step_end',
        python_callable=end_process,
        dag=dag
    )
    step_start>>step_load>>step_end