import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay


class APIH(object):
    def __init__(self):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'API_conf_win')
        # path = config.getConfig('SubConfigPath', 'API_conf_linux')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def setAPIH(self):
        API_table = self.getConfig('API_table', 'table')
        td = TradingDay()
        lastTradingDay = td.getLastTradingDay()
        sqls = "select * from %s where Occur_Date = '%s'" % (API_table, lastTradingDay)
        return sqls


if __name__ == '__main__':
    m = APIH()
    sqls = m.setAPIH()
    from Common.MySQLConnector import MySQLConnector
    ms = MySQLConnector()
    connection = ms.getConn()
    import pandas as pd
    df = pd.read_sql(sql=sqls, con=connection)
    print(df)

