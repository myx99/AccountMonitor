from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from Monitor.UnusualValuePickup import API_data_lookup, BPI_data_lookup


 # ---------- Daily ANRM data generate and import to MySQL ------------
#  Get last trade day
td = TradingDay()
lastTradingDay = td.getLastTradingDay()
# tradeday = "%s-%s-%s" % (lastTradingDay[:4], lastTradingDay[4:6], lastTradingDay[-2:])
print(lastTradingDay)

# Import by designated day, use only when amend history data, for this api process, time format is yyyymmdd
# td = "20181015"


# p = 'FB0006'
# print("========================================")
# print("Product ID : %s" % p)
# print("Static based on the end data of : %s" % enddate)
#
# uvpa = API_data_lookup(p, enddate)
# an = uvpa.duration_annualized_return()
# result = uvpa.diagnosis(an)
# angap = result[0]
# status = result[-1]
#
# print("Annualized - Target = %.4f" % angap)
# print("Alert Level: %s" % status)
# print("========================================")

bpidl = BPI_data_lookup()
product_list_tmp = bpidl.getConfig("Duration_AnnualizedReturn", "product_list")
product_list = product_list_tmp.split(", ")
print(product_list)

for p in product_list:
    print("========================================")
    print("Product ID : %s" % p)
    print("Static based on the end data of : %s" % lastTradingDay)
    uvpa = API_data_lookup(p, lastTradingDay)
    an = uvpa.duration_annualized_return()
    result = uvpa.diagnosis(an)
    targetAnnualizedReturn = result[0]
    angap = result[1]
    status = result[2]
    startdate = result[3]
    print("Annualized - Target = %.4f" % angap)
    print("Alert Level: %s" % status)
    print("========================================")
    # Compose Array
    insert_data = (lastTradingDay, p, startdate, an, targetAnnualizedReturn, angap, status)
    print(insert_data)

    uvpa.AN_monitor_Insert(insert_data)