from datetime import datetime 
from airflow import DAG 
from airflow.operators.dummy import DummyOperator 
from airflow.operators.python import BranchOperator 

def choose_branch(**kwargs): 
    if datetime.now().weekday() < 5: 
        return 'weekday_task' 
    else: 
        return 'weekend_task' 
        
dag = DAG( 
    dag_id='branch_operator_example', 
    start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', ) 
    
start = DummyOperator(task_id='start', dag=dag) 

branch = BranchOperator( 
    task_id='branch', 
    python_callable=choose_branch, 
    provide_context=True, 
    dag=dag, 
) 

weekday_task = DummyOperator(task_id='weekday_task', dag=dag) 
weekend_task = DummyOperator(task_id='weekend_task', dag=dag) 

end = DummyOperator(task_id='end', dag=dag) 

start >> branch >> [weekday_task, weekend_task] >> end