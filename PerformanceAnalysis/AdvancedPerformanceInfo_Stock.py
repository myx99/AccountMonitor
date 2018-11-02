import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from SQLStatement.API_select import APIS
import datetime
import numpy as np


class Stock(object):
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

    def portfolioVolatility(self):
        td = TradingDay()
        tradedays = td.getDuration(self.startdate, self.enddate)
        df_pcntg = pd.DataFrame()
        df_pcntg['TRADE_DT'] = tradedays
        df_pcntg['portfolio'] = float(0)

        oc = OracleConnector()
        connection = oc.getConn()
        for stock in self.stocklist:
            sqls = """select TRADE_DT, S_DQ_PCTCHANGE "%s" from wind.AShareEODPrices where S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s'and TRADE_DT <= '%s' order by TRADE_DT"""\
                   % (stock, stock, self.startdate, self.enddate)
            df_stock = pd.read_sql(sql=sqls, con=connection)
            weight = self.df_input.loc[self.df_input['VC_SCDM'] == stock, 'EN_SZZJZ'].values
            df_stock[stock] = df_stock[stock].astype(float) * weight / 100
            # print(df_stock)
            df_pcntg = pd.merge(df_pcntg, df_stock, how='outer', on='TRADE_DT')
            df_pcntg = df_pcntg.fillna(0)
            df_pcntg['portfolio'] += df_pcntg[stock]
        oc.closeConn()

        # print(df_pcntg)
        vol = df_pcntg['portfolio'].std() * np.sqrt(20)
        print("Portfolio Volatility: %.4f" % vol)
        return "%.4f" % vol


if __name__ == '__main__':
    p = "FB0003"
    m = APIS()
    sqls = m.stock(p,'20181101')
    ms = MySQLConnector()
    connection = ms.getConn()
    df = pd.read_sql(sql=sqls, con=connection)
    s = Stock(df)
    s.portfolioVolatility()
