import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
import pandas as pd


class APIS(object):

    def __init__(self):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'API_conf_win')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')


    def getConfig(self, section, key):
        return self.cp.get(section, key)


    def setAPIS(self, productID):
        BPI_table = self.getConfig('BPI_table', 'table')
        self.sqls = "select * from %s where Product_ID = '%s'" % (BPI_table, productID)
        return self.sqls



if __name__ == '__main__':
    p = "FB0009"
    m = APIS()
    sqls = m.setAPIS(p)
    ms = MySQLConnector()
    connection = ms.getConn()
    df = pd.read_sql(sql=sqls, con=connection)
    print(df)








