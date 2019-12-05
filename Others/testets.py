from Common.TradingDay import TradingDay
from Common.OracleConnector import OracleConnector
import pandas as pd
import numpy as np
from datetime import datetime


class marketdata(object):
    def __init__(self, startdate):
        # get trading days
        td = TradingDay()
        enddate = td.getLastTradingDay()
        self.tradedays = td.getDuration(startdate, enddate)

        # get stock list
        oc = OracleConnector()
        self.connection = oc.getConn()
        sqls_stocklist = "select F16_1090,OB_OBJECT_NAME_1090,F23_1090,F27_1090 from WIND.TB_OBJECT_1090 where F4_1090='A'and F19_1090='0' and F21_1090='1' order by F16_1090 asc"
        self.df_stocklist = pd.read_sql(sql=sqls_stocklist, con=self.connection)

    def getMarketData(self, count):
        stocklist = self.df_stocklist['F16_1090']
        df_price = pd.DataFrame()
        df_price['TRADE_DT'] = self.tradedays
        # print(df_price)

        for stock in stocklist[:count]:
            print(stock)
            sqls = """select F2_1425,F4_1425,F5_1425,F6_1425,F7_1425,F8_1425,F9_1425 from WIND.TB_OBJECT_1425, WIND.TB_OBJECT_1090 where F1_1425=F2_1090 and f16_1090 ='%s'and  F2_1425> '20190101' and F4_1090= 'A' order by F2_1425 asc""" \
                   % stock
            df_stock = pd.read_sql(sql=sqls, con=self.connection)
            stock_temp = '%s' % stock
            df_price[stock_temp] = df_stock['F4_1425']

        # df_price.to_csv('D:\\app\\test.csv', sep=',', header=True, index=True)
        print(df_price)


class calculation(object):
    def __init__(self):
        pass

    def closeprice(self,k1,k2,k3,decay,combo,k0):
        # df = pd.read_csv('D:\\app\\matlab_comparison\\close.csv')
        df = pd.read_csv('/home/PerformanceAnalysis/Performance/AccountMonitor/Others/close.csv')
        # print(df)
        matrix = df.as_matrix()
        size = np.shape(matrix)
        [rows, cols] = size
        matrix_temp = np.zeros(size)
        # print(matrix_temp)

        tic = datetime.now()
        for i in range(cols):
            for j in range(rows-k0, rows):
                x1 = matrix[j - k1 + 1:j + 1, i]
                x2 = matrix[j - k2 - k1 + 1:j - k2 + 1, i]
                x3 = matrix[j - k3 - k1 + 1:j - k3 + 1, i]
                y1 = x1 / x2
                y2 = x1 / x3
                # print(x1, x2, x3)

                corr = np.corrcoef(y1, y2)
                # print(corr)

                matrix_temp[j, i] = -corr[0][1]
        print(matrix_temp)
        toc = datetime.now()
        print('Processing time: %f seconds' % (toc - tic).total_seconds())

if __name__ == '__main__':
    # startdate = '20190102'
    # mkt = marketdata(startdate)
    # mkt.getMarketData(100)

    cal = calculation()
    cal.closeprice(5, 1, 13, 1, 1, 1422)
