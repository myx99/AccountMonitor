from Common.GlobalConfig import GlobalConfig
from Common.OracleConnector import OracleConnector
import pandas as pd
import datetime


class TradingDay(object):
    def __init__(self):
        self.today = datetime.date.today().strftime("%Y%m%d")


    def getLastTradingDay(self):
        oc = OracleConnector()
        connection = oc.getConn()
        sqls = "select * from (select trade_days from wind.asharecalendar where S_INFO_EXCHMARKET = 'SZSE' and trade_days < '%s' order by trade_days desc) where rownum <=1" % self.today
        df = pd.read_sql(sql=sqls, con=connection)
        tradeday = df['TRADE_DAYS'][0]
        oc.closeConn()
        return tradeday


    def getDuration(self, start, end):
        oc = OracleConnector()
        connection = oc.getConn()
        sqls = "select trade_days from wind.asharecalendar where S_INFO_EXCHMARKET = 'SZSE' and trade_days >= '%s' and trade_days <= '%s' order by trade_days" % (start, end)
        df = pd.read_sql(sql=sqls, con=connection)
        tradedays = df['TRADE_DAYS']
        oc.closeConn()
        return tradedays


if __name__ == '__main__':
    m = TradingDay()
    print(m.getDuration("20160909","20181018"))
    print(m.getLastTradingDay())



