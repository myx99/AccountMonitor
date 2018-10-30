import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
import pandas as pd


class APIS(object):
    def __init__(self):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'API_conf_win')
        # print(path)
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')
        td = TradingDay()
        self.ltd = td.getLastTradingDay()

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def setAPIS(self, productID, EndDate=None):
        BPI_table = self.getConfig('BPI_table', 'table')
        if EndDate is None:
            EndDate = self.ltd
        td = "%s-%s-%s" % (EndDate[:4], EndDate[4:6], EndDate[-2:])
        self.sqls = "select * from %s where Product_ID = '%s' and Occur_Date <= '%s' order by Occur_Date" % (BPI_table, productID, td)
        return self.sqls

    def setOriginal(self, productID, EndDate=None):
        valuation_table = self.getConfig('Valuation_Table_Info', 'table')
        column_productid = self.getConfig('Valuation_Table_Info', 'column_productid')
        column_date = self.getConfig('Valuation_Table_Info', 'column_date')
        if EndDate is None:
            EndDate = self.ltd
        td = "%s-%s-%s" % (EndDate[:4], EndDate[4:6], EndDate[-2:])
        self.sqls = "select * from %s where %s = '%s' and left(%s,10) = '%s'" \
                    % (valuation_table, column_productid, productID, column_date, td)
        return self.sqls

if __name__ == '__main__':
    p = "FB0008"
    m = APIS()
    # sqls = m.setAPIS(p,'20180609')
    sqls = m.setOriginal(p,'20181023')
    ms = MySQLConnector()
    connection = ms.getConn()
    df = pd.read_sql(sql=sqls, con=connection)
    print(df)








