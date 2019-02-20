import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from SQLStatement.API_select import APIS
import datetime
import numpy as np


class StockFilter(object):
    def __init__(self, productID, EndDate=None):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'StressTesting_win')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')
        td = TradingDay()
        ltd = td.getLastTradingDay()
        self.productID = productID
        if EndDate is None:
            self.EndDate = ltd
        else:
            self.EndDate = EndDate

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def prefix2sql_general(self, prefix):
        valuation_table = self.getConfig('Valuation_Table_Info', 'table')
        column_productid = self.getConfig('Valuation_Table_Info', 'column_productid')
        column_date = self.getConfig('Valuation_Table_Info', 'column_date')
        column_kmdm = self.getConfig('Valuation_Table_Info', 'column_KMDM')

        td = "%s-%s-%s" % (self.EndDate[:4], self.EndDate[4:6], self.EndDate[-2:])

        sqls = "select distinct * from %s where %s = '%s' and left(%s,10) = '%s' and %s like '%s_%%' " \
                    % (valuation_table, column_productid, self.productID, column_date, td, column_kmdm, prefix)
        return sqls

    def prefix2sql_filterout(self, prefix, filterout):
        valuation_table = self.getConfig('Valuation_Table_Info', 'table')
        column_productid = self.getConfig('Valuation_Table_Info', 'column_productid')
        column_date = self.getConfig('Valuation_Table_Info', 'column_date')
        column_kmdm = self.getConfig('Valuation_Table_Info', 'column_KMDM')

        td = "%s-%s-%s" % (self.EndDate[:4], self.EndDate[4:6], self.EndDate[-2:])

        sqls = "select distinct * from %s where %s = '%s' and left(%s,10) = '%s' and %s like '%s_%%' and %s not like '%s_%%' " \
                    % (valuation_table, column_productid, self.productID, column_date, td, column_kmdm, prefix, column_kmdm, filterout)
        return sqls

    def getdf(self, sqls):
        prefix_alias_list = self.getConfig('StockCategory', 'prefix_list')
        SZ_SME_prefix = self.getConfig('StockCategory', 'SZ_SME_prefix')
        prefix_alias_list = prefix_alias_list.split(", ")
        for p in prefix_alias_list:
            prefix = self.getConfig('StockCategory', p)
            if p == 'SZ_mainboard_prefix':
                self.prefix2sql_filterout()
        ms = MySQLConnector()
        connection = ms.getConn()
        df = pd.read_sql(sql=sqls, con=connection)
        return df

if __name__ == '__main__':
    product = "FB0003"
    prefix1 = 11020101
    prefix2 = 11023101
    filterout = 11023101002
    date = "20190211"
    m = StockFilter()
    sqls1 = m.prefix2sql_general(product, prefix1, date)
    sqls2 = m.prefix2sql_SZmainboard(product, prefix2, filterout, date)
    ms = MySQLConnector()
    connection = ms.getConn()
    df1 = pd.read_sql(sql=sqls1, con=connection)
    df2 = pd.read_sql(sql=sqls2, con=connection)
    print(df1, df2)

    general_prefix_list = ['SH_mainboard_prefix', 'SZ_mainboard_prefix', 'SZ_SME_prefix', 'SZ_GEM_prefix', 'SZ_HK_connect_prefix']
    for gpl in general_prefix_list:
        if gpl == 'SZ_mainboard_prefix':
            sqls = m.prefix2sql_general(product, prefix1, date)
