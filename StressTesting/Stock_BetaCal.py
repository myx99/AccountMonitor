from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
import datetime
from StressTesting.StockFilter import StockFilter
from Common.GlobalConfig import GlobalConfig
import configparser
from Common.TradingDay import TradingDay
import numpy as np


class Stock_BetaCal(object):
    def __init__(self):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'StressTesting_win')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')

        # database init
        self.oc = OracleConnector()
        self.oracle_connection = self.oc.getConn()
        self.ms = MySQLConnector()
        self.mysql_connection = self.ms.getConn()

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def Beta(self, enddate, index, df_list):
        if not index:
            return None
        sqls_td_days_Ashare = """select TRADE_DAYS "TRADE_DT" from
                                  (select distinct * from wind.AShareCalendar where S_INFO_EXCHMARKET = 'SZSE' and TRADE_DAYS <= '%s'order by TRADE_DAYS desc)  where rownum <= 120"""\
                                    % (enddate)
        sqls_td_days_Hshare = """select TRADE_DAYS "TRADE_DT" from
                                  (select distinct * from wind.AShareCalendar where S_INFO_EXCHMARKET = 'SZN' and TRADE_DAYS <= '%s'order by TRADE_DAYS desc)  where rownum <= 120""" \
                                    % (enddate)
        df_120td_Ashare = pd.read_sql(sql=sqls_td_days_Ashare, con=self.oracle_connection)
        df_120td_Hshare = pd.read_sql(sql=sqls_td_days_Hshare, con=self.oracle_connection)
        # print(df_120td_Ashare)
        # print("data df start")
        # print(df_120td_Ashare, df_120td_Hshare)
        # print("data df end")
        startdate = df_120td_Ashare['TRADE_DT'].values[-1]
        startdate_hk = df_120td_Hshare['TRADE_DT'].values[-1]
        # print(startdate, startdate_hk)

        df_result = pd.DataFrame()
        df_result['Index'] = index
        value_by_index_list = []
        beta_by_index_list = []
        for i, df in zip(index, df_list):
             # get stock list
            df.loc[df['L_SCLB'] == 1, 'VC_SCDM'] += '.SH'
            df.loc[df['L_SCLB'] == 2, 'VC_SCDM'] += '.SZ'
            stocklist = df['VC_SCDM'].values

            # get index data
            index = self.getConfig('StockIndex', i)
            if i == "SZ_HK_connect_prefix":
                sqls = """select distinct L_DATE as TRADE_DT, round((EN_LAST_PRICE - EN_YESTERDAY_CLOSE_PRICE)/EN_YESTERDAY_CLOSE_PRICE, 4) as "%s"
                           from O32_THISSTOCKINFO where VC_INTER_CODE = "%s" and L_DATE >= '%s' and L_DATE <= '%s' order by L_DATE""" \
                       % (index, index, startdate, enddate)
                df_index = pd.read_sql(sql=sqls, con=self.mysql_connection)
            else:
                sqls = """select distinct TRADE_DT, S_DQ_PCTCHANGE "%s" from wind.AIndexEODPrices where S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s'and TRADE_DT <= '%s' order by TRADE_DT""" \
                   % (index, index, startdate, enddate)
                df_index = pd.read_sql(sql=sqls, con=self.oracle_connection)
            # print(df_index)
            beta_list = []
            for stock in stocklist:
                if stock[-3:] == '.SH' or stock[-3:] == '.SZ':
                    sqls = """select distinct TRADE_DT, S_DQ_PCTCHANGE "%s" from wind.AShareEODPrices where S_INFO_WINDCODE = '%s' and TRADE_DT >= '%s' and TRADE_DT <= '%s' order by TRADE_DT""" \
                           % (stock, stock, startdate, enddate)
                    df_stock = pd.read_sql(sql=sqls, con=self.oracle_connection)

                    sqls_suspend = """select distinct * from wind.AShareEODPrices where  S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s' and TRADE_DT <= '%s' and S_DQ_TRADESTATUS = '停牌'""" \
                           % (stock, startdate, enddate)
                    df_stock_suspend = pd.read_sql(sql=sqls_suspend, con=self.oracle_connection)

                else:
                    sqls = """select distinct L_DATE as TRADE_DT, round((EN_LAST_PRICE - EN_YESTERDAY_CLOSE_PRICE)/EN_YESTERDAY_CLOSE_PRICE, 4) as "%s"
                              from O32_THISSTOCKINFO where VC_REPORT_CODE = "%s" and L_DATE >= '%s' and L_DATE <= '%s' order by L_DATE""" \
                           % (stock, stock, startdate, enddate)
                    df_stock = pd.read_sql(sql=sqls, con=self.mysql_connection)
                    df_stock['TRADE_DT'] = df_stock['TRADE_DT'].astype(str)

                    sqls_suspend = """select distinct * from O32_THISSTOCKINFO where L_DATE >= '%s' and L_DATE <= '%s' and VC_REPORT_CODE = "%s" and L_MARKET_DEAL_AMOUNT = 0""" \
                           % (startdate, enddate, stock)
                    df_stock_suspend = pd.read_sql(sql=sqls_suspend, con=self.mysql_connection)
                # print(df_stock)
                # print(df_stock_suspend)

                if len(df_stock) != 120:
                    beta = 1
                elif not df_stock_suspend.empty:
                    beta = 1
                else:
                    beta = df_stock[stock].cov(df_index[index]) / df_index[index].var()
                # print("Beta of %s is %s" % (stock, beta))
                beta_list.append(beta)
            df['Beta'] = beta_list
            df['Beta_by_value'] = df['Beta'] * df['EN_SZ']
            # print(df)

            value_by_index = df['EN_SZ'].sum()
            value_by_index_list.append(value_by_index)
            beta_by_index = np.average(df['Beta'], weights=df['EN_SZ'])
            beta_by_index_list.append(beta_by_index)

        df_result['value_by_index'] = value_by_index_list
        df_result['beta_by_index'] = beta_by_index_list
        df_result['Index'] = df_result['Index'].apply(lambda x: x[:-7])

        self.oc.closeConn()
        self.ms.closeConn()
        return df_result


if __name__ == '__main__':
    product = "FB0008"
    date = "20190211"
    m = StockFilter(product, date)
    e, x, y = m.getdf()
    # print(x)
    # print(y)
    n = Stock_BetaCal()
    z = n.Beta(e, x, y)
    print(z)
