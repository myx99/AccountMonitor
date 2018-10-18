import pymysql
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from pandas.io import sql


def db_insert(dataframe, tablename):

    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='Matao_2012', db='test', use_unicode=True, charset="utf8")
    cur = conn.cursor()

    engine = create_engine('mysql+pymysql://root:Matao_2012@localhost:3306/test?charset=utf8')

    pd.io.sql.to_sql(dataframe, tablename, engine, if_exists='append', index=False, chunksize=1000)

    cur.close()
    conn.close()

    print("Data inserted")

# test main
# df = pd.DataFrame(np.arange(12))
# db_insert(df)
