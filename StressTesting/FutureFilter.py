import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from SQLStatement.API_select import APIS
import datetime
import numpy as np


class FutureFilter(object):
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

    def prefix2sql(self, prefixlist):
        valuation_table = self.getConfig('Valuation_Table_Info', 'table')
        column_productid = self.getConfig('Valuation_Table_Info', 'column_productid')
        column_date = self.getConfig('Valuation_Table_Info', 'column_date')
        column_kmdm = self.getConfig('Valuation_Table_Info', 'column_KMDM')

        EndDate = self.EndDate
        td = "%s-%s-%s" % (EndDate[:4], EndDate[4:6], EndDate[-2:])

        ffc_List = []
        for future in prefixlist:
            if future != prefixlist[-1]:
                ffc = "%s like '%s_%%' or" % (column_kmdm, future)
            else:
                ffc = "%s like '%s_%%'" % (column_kmdm, future)
            ffc_List.append(ffc)
        FuzzyCondition = ' '.join(ffc_List)
        sqls = "select distinct * from %s where %s = '%s' and left(%s,10) = '%s' and (%s)" \
                    % (valuation_table, column_productid, self.productID, column_date, td, FuzzyCondition)
        return sqls

    def get_sqls(self):
        FuturePrefixList = self.getConfig('DerivativeCategory', 'Finacial_Future_prefix_List')
        FuturePrefixList = FuturePrefixList.split(", ")

        FutureSqls = self.prefix2sql(FuturePrefixList)

        return FutureSqls

if __name__ == '__main__':
    product = "FB0001"
    date = "20190228"
    ff = FutureFilter(product, date)
    futuresqls = ff.get_sqls()
    # print(futuresqls)
    ms = MySQLConnector()
    ms_connection = ms.getConn()
    df_future = pd.read_sql(sql=futuresqls, con=ms_connection)
    print(df_future)

