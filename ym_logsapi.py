import pandas as pd
import json
import requests
import datetime
from datetime import date
import sqlalchemy
from io import StringIO

headers = {
    'Authorization': 'OAuth ****',
    }

date1 = date.today() + datetime.timedelta(days=-14)
date2 = date.today() + datetime.timedelta(days=-1)
start_of_url = 'https://api-metrika.yandex.ru/management/v1/counter/****/'
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
oauth_token = '****'

# запрос на формирование
url_create = start_of_url + 'logrequests?' + 'date1=' + date1 + '&date2=' + date2 \
             + '&fields=' + fields + '&source=' + source + '&oauth_token=' + oauth_token

result = requests.post(url_create, headers=headers)  # запрос к API
result = result.text.encode('utf-8').decode('utf-8')  # расшифровка ответа
result_to_dict = json.loads(result)  # превращаем ответ в словарь python, чтобы потом вытащить requestID

# возвращаем текущие обращения к серверу
url_get = start_of_url+'logrequests?'
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

download_url = start_of_url+'/logrequest/'+str(request_id)+'/part/0'+'/download?&oauth_token='+oauth_token
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

# удаляем сформированные отчеты с сервера
delete_url = start_of_url+'/logrequest/'+str(request_id)+'/clean?oauth_token='+oauth_token
delete = requests.post(delete_url, headers=headers)
json.loads(delete.text.encode('utf-8').decode('utf-8'))

engine = sqlalchemy.create_engine('postgresql://postgres:****@localhost:5432/learning')
connection = engine.raw_connection()
cursor = connection.cursor()
#query = ''' delete from forpython.ym where datetime >= current_date - interval '2 days' '''
#cursor.execute(query)

df.to_sql(
    name='ym',
    con=engine,
    index=False,
    schema='forpython',
    chunksize=500,
    if_exists='append'
    )
