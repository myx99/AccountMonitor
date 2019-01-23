from Common.TradingDay import TradingDay
import pandas as pd
import datetime



startdate = "20180105"
enddate1 = "20181231"
enddate2 = "20190104"
nav_1 = 1.0932
nav_2 = 1.0946

# date format
s = datetime.datetime.strptime(startdate, '%Y%m%d')
e1 = datetime.datetime.strptime(enddate1, '%Y%m%d')
e2 = datetime.datetime.strptime(enddate2, '%Y%m%d')

# get duration
td = TradingDay()
tradedays_2018yearend = td.getDuration(startdate, enddate1)
tradedays_1year = td.getDuration(startdate, enddate2)
naturaldays_2018yearend = e1 - s
naturaldays_1year = e2 -s

# by trade days
an1 = nav_1 * 365 / tradedays_2018yearend
an2 = nav_2 * 365 / tradedays_1year

# by natural days
an3 = nav_1 * 365 / naturaldays_2018yearend
an4 = nav_2 * 365 / naturaldays_1year


print(an1, an2, an3, an4)