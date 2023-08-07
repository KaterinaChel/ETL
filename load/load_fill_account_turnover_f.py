import psycopg2
from airflow.models import Variable

def execute_fill_account_turnover():
    sql_file = '/home/system/doc/task2/for_postgresql/procedure_fill_account_turnover_f.sql'
    connection_name = Variable.get("postgres_connection")
    connection = psycopg2.connect(connection_name)


    with connection.cursor() as cursor:
        
        with open(sql_file, 'r') as file:
            sql_script = file.read()
            cursor.execute(sql_script)




            connection.commit()
    cursor.close()


    
    
