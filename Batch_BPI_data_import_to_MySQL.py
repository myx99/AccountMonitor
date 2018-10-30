from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from PerformanceAnalysis.DailyGenerate_BasicPerformanceInfo import DGBPI


 # ---------- Batch BPI data generate and import to MySQL ------------

#  Get product list fromm config.ini
gc = GlobalConfig()
product_list_tmp = gc.getConfig('product', 'list')
product_list = product_list_tmp.split(", ")
print(product_list)


#  Get tradedays since product founded
td = TradingDay()
for p in product_list:
    tradedays = td.getProductDurationSinceFounded(p)
    for tradeday in tradedays:
        t = "%s-%s-%s" % (tradeday[:4], tradeday[4:6], tradeday[-2:])
        dgbpi = DGBPI()
        dgbpi.composeArray(p, t)
        dgbpi.insertArray()
