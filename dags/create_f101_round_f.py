from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from load_fill_f_101_out import execute_f_101
from airflow.operators.python_operator import PythonOperator

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
    'execute_stored_procedures_f101_round_f',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
)

load_procedures_f_101 = PythonOperator(
    task_id='execute_f_101',
    python_callable=execute_f_101,
    dag=dag,
)

sql_code = f'''
    call dm.fill_f101_round_f('2018-01-01')
    '''
insert_f_101 = PostgresOperator(
    task_id="f101_round_f",
    sql=sql_code,  
    postgres_conn_id='postgres_ds',  
    autocommit=True,  
    dag=dag,
        )

load_procedures_f_101 >> insert_f_101

