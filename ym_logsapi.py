import pandas as pd
from pandas import json_normalize
import json
import requests
from datetime import date

headers = {
    #'GET': '/management/v1/counters HTTP/1.1',
    #'Host': 'api-metrika.yandex.net',
    'Authorization': 'OAuth AgAAAAAoVX5fAAaY0CYtJ-9qfki_tcfmAXyYLhA',
    #'Content-Type': 'application/x-yametrika+json',
    #'Content-Length': '123'
    }

today = date.today()
start_of_url = 'https://api-metrika.yandex.ru/management/v1/counter/55237969/'
date1 = '2018-01-01'
date2 = today.strftime('%Y-%d-%m')
fields = "ym:s:clientID,ym:s:dateTime,ym:s:lastTrafficSource,ym:s:visitDuration,ym:s:pageViews"
source = 'visits'
oauth_token = 'AgAAAAAoVX5fAAaY0CYtJ-9qfki_tcfmAXyYLhA'

# запрос на формирование
url_create = start_of_url+'logrequests?'+'date1='+date1+'&date2='+date2+'&fields='+fields+'&source='+source+'&oauth_token='+oauth_token

result = requests.post(url_create, headers=headers) # запрос к API
result = result.text.encode('utf-8').decode('utf-8') # расшифровка ответа
result_to_dict = json.loads(result) # превращаем ответ в словарь python, чтобы потом вытащить requestID

# возвращаем текущие обращения к серверу
url_get = start_of_url+'logrequests?'
result = requests.get(url_get, headers=headers)
result = result.text.encode('utf-8').decode('utf-8') # расшифровка ответа

result = json.loads(result) # превращаем ответ в словарь python, чтобы потом вытащить кол-во частей
request_id = result['requests'][0]['request_id']

if result['requests'][0]['status'] == 'processed':
    download_url = start_of_url+'/logrequest/'+str(request_id)+'/part/0'+'/download?&oauth_token='+oauth_token
    download_result = requests.get(download_url, headers=headers)                      # запрос к API
    download_result = download_result.text.encode('utf-8').decode('utf-8')  # расшифровка ответа
else: print('Отчет еще не сформирован')

# удаляем сформированные отчет с сервера
delete_url = start_of_url+'/logrequest/'+str(request_id)+'/clean?oauth_token='+oauth_token
delete = requests.post(delete_url,headers=headers)
json.loads(delete.text.encode('utf-8').decode('utf-8'))

from io import StringIO
df = pd.read_csv(StringIO(download_result), sep='\t', header=0)

df = df.rename(columns =
               {'ym:s:clientID' : 'client_id',
                'ym:s:dateTime' : 'datetime',
                'ym:s:lastTrafficSource' : 'last_traffic_source',
                'ym:s:visitDuration' : 'visit_duration',
                'ym:s:pageViews' : 'page_views'}) # переименовать столбцы
print(df)
df = df.astype({'client_id': str, 'datetime': 'datetime64[ns]', 'last_traffic_source':str, 'visit_duration':float, 'page_views':float})
print(df)

import sqlalchemy
from sqlalchemy.dialects import postgresql
engine = sqlalchemy.create_engine('postgresql://postgres:QasdWsx54@localhost:5432/learning base')

from sqlalchemy.types import Integer, Text, String, DateTime

df.to_sql(
    name='ym',
    con=engine,
    index=False,
    schema='testing',
    chunksize=500,
    if_exists='replace'
    )



