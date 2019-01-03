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

# Import by designated day, use only when amend history data
# td = "2018-10-15"

#  Use product id and trade day to generate basic info and finish import to Basic Performance Info table
for p in product_list:
    print(p)
    dgapii = DGAPI_insert(p, lastTradingDay)
    dgapii.insertArray()
