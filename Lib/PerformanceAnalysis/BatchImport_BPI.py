import datetime
import pandas as pd
from Lib.Common.OracleConnector import OracleConnector
import Lib.PerformanceAnalysis.DailyGenerate_BasicPerformanceInfo as DGBPI
from Lib.Common.GlobalConfig import GlobalConfig


def tradeDayCollect(start, end):
    m = OracleConnector()
    connection = m.getConn()
    sqls = "select trade_days from wind.asharecalendar where S_INFO_EXCHMARKET = 'SZSE' and trade_days >= '%s' and trade_days <= '%s' order by trade_days" % (start, end)
    df = pd.read_sql(sql=sqls, con=connection)
    tradedays = df['TRADE_DAYS']
    print(tradedays)
    return tradedays
    m.closeConn()


def singleImport(pid):
    m = GlobalConfig()
    start_temp = m.getConfig(pid, 'Start_date')
    start = start_temp.replace("-", "")
    end_temp = m.getConfig(pid, 'End_date')
    end_temp2 = end_temp.replace("-", "")
    # today = time.strftime("%Y%m%d")
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    # print(yesterday)
    if end_temp2 == "notyet":
        end = yesterday
    elif yesterday > end_temp2:
        end = end_temp2
    # print(start, end)
    tradedays = tradeDayCollect(start, end)
    for t in tradedays:
        td = "%s-%s-%s" % (t[:4], t[4:6], t[-2:])
        array = DGBPI.composeArray(pid, td)
        print(array)
        DGBPI.insertArray(array)


def singleDayImport(pid, day):
    array = DGBPI.composeArray(pid, day)
    # print(array)
    DGBPI.insertArray(array)


# pid = 'FB0001'
# singleImport(pid)

# batch import
m = GlobalConfig()
product_list_tmp = m.getConfig('product', 'list')
product_list = product_list_tmp.split(", ")
for p in product_list:
    singleImport(p)

# day import
pid = 'FB0001'
day = '2018-09-21'
singleDayImport(pid, day)