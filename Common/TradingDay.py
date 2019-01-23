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

    def getNaturalDayCounts(self, start, end):
        date1 = datetime.datetime.strptime(start, "%Y%m%d")
        date2 = datetime.datetime.strptime(end, "%Y%m%d")
        num = (date2 - date1).days
        return num + 1

    def getTradingDayCounts(self, start, end):
        df = self.getDuration(start, end)
        return len(df)

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

    def getProductNaturalDurationSinceFounded(self, productid, enddate=None):
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
        date1 = datetime.datetime.strptime(start, "%Y%m%d")
        date2 = datetime.datetime.strptime(end, "%Y%m%d")
        num = (date2 - date1).days
        return num + 1

    def getProductTradingDayCounts(self, productid, enddate=None):
        df = self.getProductDurationSinceFounded(productid, enddate)
        return len(df)

    def getProductNaturalDayCounts(self, productid, enddate=None):
        num = self.getProductNaturalDurationSinceFounded(productid, enddate)
        return num


if __name__ == '__main__':
    m = TradingDay()
    # print(m.getDuration("20160909", "20181018"))
    # print("last trading day: ")
    # print(m.getLastTradingDay())
    # print("trading day list since founded: ")
    # print(m.getProductDurationSinceFounded("FB0003", "20190117"))
    # print("natural days: ")
    # print(m.getNaturalDayCounts("20170526", "20190116"))
    # print("trading days: ")
    # print(m.getTradingDayCounts("20170526", "20190116"))
    print(m.getProductTradingDayCounts("FB0003", "20190116"))
    print(m.getProductNaturalDayCounts("FB0003", "20190116"))




