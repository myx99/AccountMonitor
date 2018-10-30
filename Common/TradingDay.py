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

    def getProductDurationSinceFounded(self, productid, enddate=None):
        gc = GlobalConfig()
        start_temp = gc.getConfig(productid, 'Start_date')
        start = start_temp.replace("-", "")
        end_temp = gc.getConfig(productid, 'End_date')
        end_temp2 = end_temp.replace("-", "")
        lasttradeday = self.getLastTradingDay()
        if enddate is None:
            if end_temp2 == "notyet":
                end = lasttradeday
            elif lasttradeday > end_temp2:
                end = end_temp2
        else:
            end = enddate
        tradedays = self.getDuration(start, end)
        return tradedays


if __name__ == '__main__':
    m = TradingDay()
    # print(m.getDuration("20160909","20181018"))
    print(m.getLastTradingDay())
    print(m.getProductDurationSinceFounded("FB0003", "20170710"))



