from Common.TradingDay import TradingDay
from Common.OracleConnector import OracleConnector
import pandas as pd

td = TradingDay()
enddate = td.getLastTradingDay()
startdate = '20190102'
tradedays = td.getDuration(startdate, enddate)

# print(tradedays)

oc = OracleConnector()
connection = oc.getConn()
sqls_stocklist = "select F16_1090,OB_OBJECT_NAME_1090,F23_1090,F27_1090 from WIND.TB_OBJECT_1090 where F4_1090='A'and F19_1090='0' and F21_1090='1' order by F16_1090 asc"
df_stocklist = pd.read_sql(sql=sqls_stocklist, con=connection)

# print(df_stocklist)

stocklist = df_stocklist['F16_1090']
df_price = pd.DataFrame()
df_price['TRADE_DT'] = tradedays
# print(df_price)

for stock in stocklist[:100]:
    sqls = """select F2_1425,F4_1425,F5_1425,F6_1425,F7_1425,F8_1425,F9_1425 from WIND.TB_OBJECT_1425, WIND.TB_OBJECT_1090 where F1_1425=F2_1090 and f16_1090 ='%s'and  F2_1425> '20190101' and F4_1090= 'A' order by F2_1425 asc""" \
           % stock
    df_stock = pd.read_sql(sql=sqls, con=connection)
    stock_temp = '%s' % stock
    df_price[stock_temp] = df_stock['F4_1425']
    # print(df_price)


print(df_price)
# df_price.to_csv('D:\\app\\test.csv', sep=',', header=True, index=True)


