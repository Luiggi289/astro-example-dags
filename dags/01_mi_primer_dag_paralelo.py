from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime

    
def start_process():
    print(" INICIO EL PROCESO!")

def load_master():
    print(" Load Master!")

def load_raw_2():
    print(" Hola Raw 2!")

def load_raw_1():
    print(" Hola Raw 1!")

with DAG(
    dag_id="mi_primer_dag", schedule="@once", start_date=datetime(2024, 1, 22), is_paused_upon_creation=False, catchup=False
) as dag:
    step_start = PythonOperator(
        task_id='step_start',
        python_callable=start_process,
        dag=dag
    )
    step_load_raw_1 = PythonOperator(
        task_id='step_load_raw_1',
        python_callable=load_raw_1,
        dag=dag
    )
    step_load_raw_2 = PythonOperator(
        task_id='step_load_raw_2',
        python_callable=load_raw_2,
        dag=dag
    )
    step_master = PythonOperator(
        task_id='step_master',
        python_callable=load_master,
        dag=dag
    )
    step_start>>step_load_raw_1
    step_start>>step_load_raw_2
    step_load_raw_1>>load_master
    step_load_raw_2>>load_master