import pandas as pd
import json
import requests
import datetime
from datetime import date
import sqlalchemy
from io import StringIO
import gspread
from airflow import DAG
from gspread_dataframe import set_with_dataframe

import os
import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 2, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'schedule_interval': '@daily',
}


def download_logs():
    headers = {
        'Authorization': Credentials.logs_authorization,
    }

    date1 = date.today() + datetime.timedelta(days=-60)
    date2 = date.today() + datetime.timedelta(days=-1)
    start_of_url = 'https://api-metrika.yandex.ru/management/v1/counter' + Credentials.ym_counter # класс Credentials содержит все ключи/пароли
    date1 = date1.strftime('%Y-%m-%d')
    date2 = date2.strftime('%Y-%m-%d')
    fields = "ym:s:clientID" \
             ",ym:s:dateTime" \
             ",ym:s:lastTrafficSource" \
             ",ym:s:visitDuration" \
             ",ym:s:pageViews" \
             ",ym:s:goalsID" \
             ",ym:s:goalsSerialNumber" \
             ",ym:s:goalsDateTime"
    source = 'visits'
    oauth_token = Credentials.logs_authorization2

    # запрос на формирование
    url_create = start_of_url + 'logrequests?' + 'date1=' + date1 + '&date2=' + date2 \
                 + '&fields=' + fields + '&source=' + source + '&oauth_token=' + oauth_token

    result = requests.post(url_create, headers=headers)  # запрос к API
    result = result.text.encode('utf-8').decode('utf-8')  # расшифровка ответа
    result_to_dict = json.loads(result)  # превращаем ответ в словарь python, чтобы потом вытащить requestID

    # возвращаем текущие обращения к серверу
    url_get = start_of_url + 'logrequests?'
    result = requests.get(url_get, headers=headers)
    result = result.text.encode('utf-8').decode('utf-8')  # расшифровка ответа

    result = json.loads(result)  # превращаем ответ в словарь python, чтобы потом вытащить кол-во частей
    request_id = result['requests'][0]['request_id']

    while True:
        url_get = start_of_url + 'logrequests?'
        result = requests.get(url_get, headers=headers)
        result = result.text.encode('utf-8').decode('utf-8')  # расшифровка ответа
        result = json.loads(result)
        # time.sleep(15)  # ставим интервал между обращениями к серверу
        if result['requests'][0]['status'] == 'processed':
            break

    download_url = start_of_url + '/logrequest/' + str(request_id) + '/part/0' + '/download?&oauth_token=' + oauth_token
    download_result = requests.get(download_url, headers=headers)  # запрос к API
    download_result = download_result.text.encode('utf-8').decode('utf-8')  # расшифровка ответа

    df = pd.read_csv(StringIO(download_result), sep='\t', header=0)

    df = df.rename(columns={
        'ym:s:clientID': 'client_id',
        'ym:s:dateTime': 'datetime',
        'ym:s:lastTrafficSource': 'last_traffic_source',
        'ym:s:visitDuration': 'visit_duration',
        'ym:s:pageViews': 'page_views',
        'ym:s:goalsID': 'goals_id',
        'ym:s:goalsSerialNumber': 'goals_serial_number',
        'ym:s:goalsDateTime': 'goals_datetime'})  # переименовать столбцы

    df = df.astype({'client_id': str,
                    'datetime': 'datetime64[ns]',
                    'last_traffic_source': str,
                    'visit_duration': float,
                    'page_views': float,
                    'goals_id': str,
                    'goals_serial_number': str,
                    'goals_datetime': str})

    # удаляем сформированные отчет с сервера
    delete_url = start_of_url + '/logrequest/' + str(request_id) + '/clean?oauth_token=' + oauth_token
    delete = requests.post(delete_url, headers=headers)
    json.loads(delete.text.encode('utf-8').decode('utf-8'))

    engine = sqlalchemy.create_engine(Credentials.postgre_engine)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    query = ''' delete from forpython.odd_ym '''
    cursor.execute(query)

    df.to_sql(
        name='odd_ym',
        con=engine,
        index=False,
        schema='forpython',
        chunksize=500,
        if_exists='append'
    )


def odd_to_dds_1():
    engine = sqlalchemy.create_engine(Credentials.postgre_engine)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    query = '''delete from forpython.dds_ym'''
    cursor.execute(query)


def odd_to_dds_2():
    engine = sqlalchemy.create_engine(Credentials.postgre_engine)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    query = '''insert into forpython.dds_ym select distinct * from forpython.odd_ym'''
    cursor.execute(query)


def dds_to_mart_1():
    engine = sqlalchemy.create_engine(Credentials.postgre_engine)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    query = '''delete from forpython.mart_ym'''
    cursor.execute(query)


def dds_to_mart_2():
    engine = sqlalchemy.create_engine(Credentials.postgre_engine)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    query = '''insert into forpython.mart_ym select client_id
    , datetime
    , last_traffic_source
    , visit_duration 
    , page_views 
    , sum( case when goals_id like '%157417987%' then 1 else 0 end ) as show_dbpage
    , sum( case when goals_id like '%157418284%' then 1 else 0 end ) as scroll_80perc
    , sum( case when goals_id like '%157418581%' then 1 else 0 end ) as click_dbpage
    , sum( case when goals_id like '%157418950%' then 1 else 0 end ) as click_github
    from forpython.dds_ym
    group by client_id
    , datetime 
    , last_traffic_source 
    , visit_duration 
    , page_views;'''
    cursor.execute(query)


def move_to_spreasheets():
    gc = gspread.service_account(
        filename=Credentials.gs_json)
    sh = gc.open_by_key(Credentials.gs_key)
    worksheet = sh.get_worksheet(0)
    cells = worksheet.range('A2:I10000')  # выбор ячеек для очистки
    for cell in cells:
        cell.value = ''
    worksheet.update_cells(cells)
    engine = sqlalchemy.create_engine(Credentials.postgre_engine)
    df = pd.read_sql_query('select * from forpython.mart_ym', con=engine)
    set_with_dataframe(worksheet, df)


with DAG(dag_id='ym_logsapi', default_args=args) as dag:
    ymlogsapi = PythonOperator(
        task_id='download_logs',
        python_callable=download_logs,
        dag=dag)
    odd_to_dds_1 = PythonOperator(
        task_id='odd_to_dds_1',
        python_callable=odd_to_dds_1,
        dag=dag)
    odd_to_dds_2 = PythonOperator(
        task_id='odd_to_dds_2',
        python_callable=odd_to_dds_2,
        dag=dag)
    dds_to_mart_1 = PythonOperator(
        task_id='dds_to_mart_1',
        python_callable=dds_to_mart_1,
        dag=dag)
    dds_to_mart_2 = PythonOperator(
        task_id='dds_to_mart_2',
        python_callable=dds_to_mart_2,
        dag=dag)
    moveto = PythonOperator(
        task_id='movetospreadsheets',
        python_callable=move_to_spreasheets,
        dag=dag
    )
    ymlogsapi >> odd_to_dds_1 >> odd_to_dds_2 >> dds_to_mart_1 >> dds_to_mart_2 >> moveto
