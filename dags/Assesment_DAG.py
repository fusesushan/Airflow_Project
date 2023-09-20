import json
from datetime import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
import csv

with DAG(
    dag_id = 'assignment_dag',
    schedule_interval = '0 0 * * *',  # Every day at 12 morning
    start_date = datetime(2023,9,19),
    catchup = False, # Default true
) as dag:


    is_api_active = HttpSensor(
        task_id = "is_api_active",
        http_conn_id = "connection_http_test",
        endpoint = 'comments/',
    )

    get_api_data = SimpleHttpOperator(
            task_id = 'get_datas',
            http_conn_id = 'connection_http_test',
            endpoint = 'comments/',
            method = 'GET',
            response_filter = lambda response: json.loads(response.text) ,
            log_response = True,
        )

    def json_to_csv(**kwargs):
            task_instance = kwargs['ti']
            response_data = task_instance.xcom_pull(task_ids="get_datas")

            if response_data:
                csv_file_path = '/home/sushan/airflow/Airflow_Assignment/datas.csv'
                with open(csv_file_path, 'w', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile)
                    header = list(response_data[0].keys())
                    csv_writer.writerow(header)
                    for item in response_data:
                        csv_writer.writerow(list(item.values()))
            else:
                print("No JSON data available in XCom to convert to CSV.")

    to_csv = PythonOperator(
        task_id="to_csv",
        python_callable=json_to_csv,
        provide_context=True,
        )

    file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath='/home/sushan/airflow/Airflow_Assignment/datas.csv',
        poke_interval=15,
        timeout=300,
        mode='poke',
        )


    move_to_tmp = BashOperator(
        task_id='move_to_tmp',
        bash_command='mv  /home/sushan/airflow/Airflow_Assignment/datas.csv /tmp/',
    )

    load_to_postgres = PostgresOperator(
        task_id="load_to_postgres",
        postgres_conn_id="postgres_assigment",
        sql="""TRUNCATE TABLE comments_table;
                COPY comments_table FROM '/tmp/datas.csv' CSV HEADER;""",
    )


    for_spark_submit = BashOperator(
        task_id='submit_spark',
        bash_command = 'spark-submit --jars /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.6.0.jar /home/sushan/airflow/scripts/pyscript.py',
    )

    from_postgres = PostgresOperator(
        task_id="read_datas",
        postgres_conn_id="postgres_assigment",
        sql="SELECT * FROM new_comments_table;",
    )

    def log_result(ti):
     result = ti.xcom_pull(task_ids='read_datas')
     for row in result:
        print(row)

    log_result_task = PythonOperator(
        task_id = "log_result",
        python_callable = log_result,
        provide_context = True,
    )

is_api_active>>get_api_data>>to_csv>>file_sensor_task>>move_to_tmp>>load_to_postgres>>for_spark_submit>>from_postgres>>log_result_task
