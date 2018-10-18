import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay


class BPIH(object):

    def __init__(self):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'BPI_conf_win')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')


    def getConfig(self, section, key):
        return self.cp.get(section, key)


    def setBPIH(self):
        BPI_table = self.getConfig('BPI_table', 'table')
        td = TradingDay()
        lastTradingDay = td.getLastTradingDay()
        self.sqls = "select * from %s where Occur_Date = '%s'" % (BPI_table, lastTradingDay)
        return self.sqls


if __name__ == '__main__':
    m = BPIH()
    sqls = m.setBPIH()
    from Common.MySQLConnector import MySQLConnector
    ms = MySQLConnector()
    connection = ms.getConn()
    import pandas as pd
    df = pd.read_sql(sql=sqls, con=connection)
    print(df)

