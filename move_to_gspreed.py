import sqlalchemy
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

gc = gspread.service_account(filename='****.json')
sh = gc.open_by_key('****')
worksheet = sh.get_worksheet(0)

engine = sqlalchemy.create_engine('postgresql://postgres:****@localhost:5432/learning')

df = pd.read_sql_query('select * from forpython.ym', con=engine)

set_with_dataframe(worksheet, df)
