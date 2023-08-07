from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from load_fill_account_turnover_f import execute_fill_account_turnover
from load_logs import load_logs
from airflow.operators.python_operator import PythonOperator
import time


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'execute_stored_procedures_dag_increment_date',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
)

load_procedures_fill_account_task = PythonOperator(
    task_id='load_procedures_fill_account',
    python_callable=execute_fill_account_turnover,
    dag=dag,
)

load_procedures_logs_task = PythonOperator(
    task_id='load_procedures_logs',
    python_callable=load_logs,
    dag=dag,
)

def create_task(start_date, days_to_increment):
    prev_task = load_procedures_logs_task  
    for i in range(days_to_increment):
        current_date = start_date + timedelta(days=i)


        sql_code = f'''
            call ds.fill_account_turnover_f('{current_date.strftime('%Y-%m-%d')}')
        '''
        task = PostgresOperator(
            task_id=f"task_{current_date.strftime('%Y-%m-%d')}",
            sql=sql_code,  
            postgres_conn_id='postgres_ds',  
            autocommit=True, 
            dag=dag,
        )

        wait_task = TimeDeltaSensor(
            task_id=f"wait_task_{current_date.strftime('%Y-%m-%d')}",
            delta=timedelta(seconds=30),
            dag=dag,
        )

        prev_task >> wait_task >> task
        prev_task = task


start_date = datetime(2018, 1, 1)
days_to_increment = 31  



load_procedures_fill_account_task >> load_procedures_logs_task
create_task(start_date, days_to_increment)
