import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from SQLStatement.API_select import APIS
import datetime
import numpy as np


class Bond(object):
    def __init__(self, df):
        df.loc[df['L_SCLB'] == 1, 'VC_SCDM'] += '.SH'
        df.loc[df['L_SCLB'] == 2, 'VC_SCDM'] += '.SZ'

        self.stocklist = df['VC_SCDM'].values
        print("Stock List : %s" % self.stocklist)

        enddate = df['D_YWRQ'].iloc[0]
        self.startdate = (enddate - datetime.timedelta(days=180)).strftime("%Y%m%d")
        self.enddate = enddate.strftime("%Y%m%d")
        print("start: %s | end: %s" % (self.startdate, self.enddate))

        self.df_input = df

