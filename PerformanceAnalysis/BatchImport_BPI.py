import datetime
from Common.GlobalConfig import GlobalConfig
from PerformanceAnalysis.DailyGenerate_BasicPerformanceInfo import composeArray, insertArray
from Common.TradingDay import TradingDay


def multiDayImport(pid):
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
    n = TradingDay()
    tradedays = n.getDuration(start, end)
    for t in tradedays:
        td = "%s-%s-%s" % (t[:4], t[4:6], t[-2:])
        array = composeArray(pid, td)
        print(array)
        insertArray(array)


def singleDayImport(pid, day):
    array = composeArray(pid, day)
    print(array)
    insertArray(array)


# # pid = 'FB0001'
# # singleImport(pid)
#
# # batch import
# m = GlobalConfig()
# product_list_tmp = m.getConfig('product', 'list')
# product_list = product_list_tmp.split(", ")
# for p in product_list:
#     singleImport(p)
#
# # day import
# pid = 'FB0001'
# day = '2018-09-21'
# singleDayImport(pid, day)