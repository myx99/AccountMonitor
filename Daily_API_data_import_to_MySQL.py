from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from PerformanceAnalysis.DailyGenerate_AdvancedPerformanceInfo import DGAPI_insert


 # ---------- Daily API data generate and import to MySQL ------------

#  Get product list fromm config.ini
gc = GlobalConfig()
product_list_tmp = gc.getConfig('product', 'list')
product_list = product_list_tmp.split(", ")
print(product_list)

#  Get last trade day
td = TradingDay()
lastTradingDay = td.getLastTradingDay()
# tradeday = "%s-%s-%s" % (lastTradingDay[:4], lastTradingDay[4:6], lastTradingDay[-2:])
print(lastTradingDay)

# Import by designated day, use only when amend history data, for this api process, time format is yyyymmdd
# td = "20181015"


#  Use product id and trade day to generate basic info and finish import to Basic Performance Info table
for p in product_list:
    print(p)
    dgapii = DGAPI_insert(p, lastTradingDay)
    dgapii.insertArray()

# amend data, please comment out after use
# tds = ["20190531", "20190603", "20190604", "20190605", "20190606", "20190610", "20190611"]
# for p in product_list:
#     print(p)
#     for t in tds:
#         print(t)
#         dgapii = DGAPI_insert(p, t)
#         dgapii.insertArray()
