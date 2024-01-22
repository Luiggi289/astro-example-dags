from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime
import os
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd

def get_connect_mongo():

    CONNECTION_STRING ="mongodb+srv://atlas:T6.HYX68T8Wr6nT@cluster0.enioytp.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING)

    return client
    
def flujo_envio():
    print(" exitosamente!")
    

def load_products():
    print(f" INICIO LOAD PRODUCTS")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["products"] 
    products = collection_name.find({})
    products_df = DataFrame(products)
    dbconnect.close()
    products_df['_id'] = products_df['_id'].astype(str)
    products_df['product_description'] = products_df['product_description'].astype(str)
    products_rows=len(products_df)
    print(f" Se obtuvo  {products_rows}  Filas")

with DAG(
    dag_id="test_print", schedule="@once", start_date=datetime(2023, 2, 1), is_paused_upon_creation=False, catchup=False
) as dag:
    envio_correo = PythonOperator(
        task_id='envio_correo',
        python_callable=flujo_envio,
        dag=dag
    )
    step_load = PythonOperator(
        task_id='load_products',
        python_callable=load_products,
        dag=dag
    )
    envio_correo>>step_load