import pandas as pd
import matplotlib.pyplot as plt
from Utilities.DB_query_NAV import Query_NAV
from data_analysis.DB_query import db_query
import numpy as np
import datetime.date as date


def annualized_return(product_id, startdate, enddate):
    # get nav increase data by day
    # df = Query_NAV(product_id, startdate, enddate)
    df = db_query(product_id, startdate, enddate, "nav")
    # print(df.dtypes)  # DEBUG

    df['NAV'] = df['NAV'].astype(float)
    df['Date'] = df['Date'].astype(date)

    mg = pd.period_range(df['Date'].iloc[0], df['Date'].iloc[-1], freq='D')
    an = pow(df.ix[len(df.index) - 1, 'NAV'] / df.ix[0, 'NAV'], 250 / len(mg)) - 1

    return "%.4f" % an


# test main
# an = annualized_return('GZFB0001', '2017-01-18', '2017-02-28')
# print(an)
