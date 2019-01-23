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
        # print("Stock List : %s" % self.stocklist)

        enddate = df['D_YWRQ'].iloc[0]
        self.startdate = (enddate - datetime.timedelta(days=180)).strftime("%Y%m%d")
        self.enddate = enddate.strftime("%Y%m%d")
        print("stock vol - start: %s | end: %s" % (self.startdate, self.enddate))

        self.df_input = df

    def portfolioVolatility(self):
        td = TradingDay()
        tradedays = td.getDuration(self.startdate, self.enddate)
        df_pcntg = pd.DataFrame()
        df_pcntg['TRADE_DT'] = tradedays
        df_pcntg['portfolio'] = float(0)
        # print(df_pcntg)

        oc = OracleConnector()
        oracle_connection = oc.getConn()
        ms = MySQLConnector()
        mysql_connection = ms.getConn()
        for stock in self.stocklist:
            if stock[-3:] == '.SH' or stock[-3:] == '.SZ':
                sqls = """select TRADE_DT, S_DQ_PCTCHANGE "%s" from wind.AShareEODPrices where S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s'and TRADE_DT <= '%s' order by TRADE_DT""" \
                       % (stock, stock, self.startdate, self.enddate)
                df_stock = pd.read_sql(sql=sqls, con=oracle_connection)
                # print(df_stock)
            else:
                sqls = """select L_DATE as TRADE_DT, round((EN_LAST_PRICE - EN_YESTERDAY_CLOSE_PRICE)/EN_YESTERDAY_CLOSE_PRICE, 4) as "%s"
                          from O32_THISSTOCKINFO where VC_REPORT_CODE = "%s" and L_DATE >= '%s' and L_DATE <= '%s' order by L_DATE""" \
                       % (stock, stock, self.startdate, self.enddate)
                df_stock = pd.read_sql(sql=sqls, con=mysql_connection)
                df_stock['TRADE_DT'] = df_stock['TRADE_DT'].astype(str)
                df_stock[stock] = df_stock[stock] * 100
                # print(df_stock)
            weight = self.df_input.loc[self.df_input['VC_SCDM'] == stock, 'EN_SZZJZ'].values
            # print(weight)
            df_stock[stock] = df_stock[stock].astype(float) * weight / 100
            # print(type(df_stock['TRADE_DT'][0]))
            df_pcntg = pd.merge(df_pcntg, df_stock, how='outer', on='TRADE_DT')
            df_pcntg = df_pcntg.fillna(0)
            df_pcntg['portfolio'] += df_pcntg[stock]
            # print(df_pcntg)
        oc.closeConn()
        ms.closeConn()

        # print(list(df_pcntg['portfolio']))
        # writer = pd.ExcelWriter("D:\PerformanceAnalysis/output.xlsx")
        # print(df_pcntg)
        # df_pcntg.to_excel(writer, 'Sheet2')
        # writer.save()
        vol = df_pcntg['portfolio'].std() * np.sqrt(20)
        print("Stock Portfolio Volatility: %.4f" % vol)
        return vol


if __name__ == '__main__':
    p = "FB0003"
    m = APIS()
    sqls = m.stock(p,'20190116')
    ms = MySQLConnector()
    connection = ms.getConn()
    df = pd.read_sql(sql=sqls, con=connection)
    # print(df)
    s = Stock(df)
    s.portfolioVolatility()
