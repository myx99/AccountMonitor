import pandas as pd
import numpy as np
import datetime as date
from Common.MySQLConnector import MySQLConnector
from SQLStatement.API_select import APIS
# from WindPy import date


def annualized_return(productID):
    m = APIS()
    sqls = m.setAPIS(productID)
    ms = MySQLConnector()
    connection = ms.getConn()
    df = pd.read_sql(sql=sqls, con=connection)
    ms.closeConn()

    column_date = 'Occur_Date'
    column_nav = 'Accumulated_NAV'

    df[column_nav] = df[column_nav].astype(float)
    df[column_date] = df[column_date]

    mg = pd.period_range(df[column_date].iloc[0], df[column_date].iloc[-1], freq='D')
    an = pow(df.ix[len(df.index) - 1, column_nav] / df.ix[0, column_nav], 250 / len(mg)) - 1

    return "%.4f" % an


# test main
an = annualized_return('FB0001')
print(an)
