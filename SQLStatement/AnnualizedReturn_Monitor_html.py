import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay


class ANRM(object):
    def __init__(self):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'monitor_win')
        # path = config.getConfig('SubConfigPath', 'monitor_linux')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def setANRM(self):
        ANRM_table = self.getConfig('Annualized_Return_DB', 'table')
        td = TradingDay()
        lastTradingDay = td.getLastTradingDay()
        # add distinct to filer out duplicate items
        sqls = "select distinct * from %s where Occur_Date = '%s'" % (ANRM_table, lastTradingDay)
        return sqls


if __name__ == '__main__':
    m = ANRM()
    sqls = m.setANRM()
    from Common.MySQLConnector import MySQLConnector
    ms = MySQLConnector()
    connection = ms.getConn()
    import pandas as pd
    df = pd.read_sql(sql=sqls, con=connection)
    print(df)

