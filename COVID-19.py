import requests
import logging
import psycopg2
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


BASE_ENDPOINT='/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'

def calculate_deaths_per_country(country):
    deaths = df[df['Country_Region'] == country]['Deaths'].sum()
    return deaths
import os

def download_covid_data(**kwargs):
    conn = BaseHook.get_connection(kwargs['conn_id'])
    url = conn.host + kwargs['endpoint'] + kwargs['exec_date'] + '.csv'
    logging.info(f'Sending get to Covid-19') #логи для отслеживания
    response = requests.get(url)
    if response.status_code == 200:
        save_path = f'/home/nikita/PycharmProjects/airflow-docker/dags/{kwargs["exec_date"]}.csv'
        logging.info(f'Succesfully')
        # проверяем, существует ли файл
        if not os.path.exists(save_path):
            # если файл не существует, создаем его
            with open(save_path,'w') as f:
                f.write('')
        # записываем данные в файл
        with open(save_path,'w') as f:
            f.write(response.text)
    else:
        raise ValueError(f'Unable to connect')


# Функция для записи данных в PostgreSQL
def write_data_to_postgres(**context):
    conn = psycopg2.connect(
        host='<postgres_1>',
        port='<5432>',
        database='<test_app>',
        user='<postgres>',
        password='<postgres>'
    )
    cursor = conn.cursor()
    # Создаем таблицу, если она не существует
    create_table_query = '''CREATE TABLE IF NOT EXISTS deaths_per_country (
                                country VARCHAR(255) PRIMARY KEY,
                                deaths INTEGER
                            );'''
    cursor.execute(create_table_query)
    conn.commit()
    # Записываем данные в таблицу
    for country in ['Russia', 'Ukraine', 'Belarus']:
        deaths = calculate_deaths_per_country(country)
        insert_query = f"INSERT INTO deaths_per_country (country, deaths) VALUES ('{country}', {deaths});"
        cursor.execute(insert_query)
        conn.commit()
    cursor.close()
    conn.close()

default_arg = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 60,
}

with DAG(
        dag_id='covid_daily_data',
        tags=['daily', 'covid-19'],
        description='Ежедневный загруз данных',
        schedule_interval='0 7 * * *',
        max_active_runs=1,
        concurrency=2,
        default_args=default_arg,
        user_defined_macros={
            'convert_date': lambda dt: dt.strftime('%m-%d-%Y')
        }

) as dag:

    # 0 полцчить дату
    EXEC_DATE = '{{convert_date(execution_date) }}'
    # 1 проверка доступности API
    check_if_data_available = HttpSensor(
        task_id='check_if_data_available',
        http_conn_id='covid-api',
        endpoint=f'{BASE_ENDPOINT}{EXEC_DATE}.csv', #парсим файлики с датой которую вычисляем
        poke_interval=60,
        timeout=600,
        soft_fail=False,
        mode='reschedule'
    )
    # 2 загрузка данных
    download_data = PythonOperator(
        task_id='download_data',
        python_callable = download_covid_data,
        op_kwargs={
            'conn_id':'covid-api',
            'endpoint': BASE_ENDPOINT,
            'exec_date': EXEC_DATE
        }
    )
    # 3 обработка этих данных
    task_russia = PythonOperator(
        task_id='russia',
        python_callable=calculate_deaths_per_country,
        op_kwargs={'country': 'Russia'},
        dag=dag
    )

    task_ukraine = PythonOperator(
        task_id='ukraine',
        python_callable=calculate_deaths_per_country,
        op_kwargs={'country': 'Ukraine'},
        dag=dag
    )

    task_belarus = PythonOperator(
        task_id='belarus',
        python_callable=calculate_deaths_per_country,
        op_kwargs={'country': 'Belarus'},
        dag=dag
    )


    # 4 перемещение в постгрес

    task_write_to_postgres = PythonOperator(
        task_id='write_to_postgres',
        python_callable=write_data_to_postgres,
        provide_context=True,
        dag=dag
    )
check_if_data_available >> download_data >> [task_russia, task_ukraine, task_belarus] >> task_write_to_postgres















