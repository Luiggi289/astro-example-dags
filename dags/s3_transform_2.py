from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime
import os

def flujo_envio():
    print(" exitosamente!")

with DAG(
    dag_id="test_print", schedule="@once", start_date=datetime(2024, 2, 1), is_paused_upon_creation=False, catchup=False
) as dag:
    envio_correo = PythonOperator(
        task_id='envio_correo',
        python_callable=flujo_envio,
        dag=dag
    )
    
    envio_correo