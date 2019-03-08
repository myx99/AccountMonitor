from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from StressTesting.DailyGenerate_StressScenarioCal import DGSSC


#  Get product list fromm config.ini
gc = GlobalConfig()
product_list_tmp = gc.getConfig('product', 'list')
product_list = product_list_tmp.split(", ")
print(product_list)

#  Get last trade day
td = TradingDay()
date = td.getLastTradingDay()
# tradeday = "%s-%s-%s" % (lastTradingDay[:4], lastTradingDay[4:6], lastTradingDay[-2:])
# date = "20190211"

#  Use product id and trade day to generate basic info and finish import to Basic Performance Info table
for p in product_list:
    dgssc = DGSSC(p, date)
    dgssc.Data_Insert()
