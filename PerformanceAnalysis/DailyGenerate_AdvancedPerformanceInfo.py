import pandas as pd
from Common.MySQLConnector import MySQLConnector
from SQLStatement.API_select import APIS
import numpy as np
import math
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from PerformanceAnalysis.AdvancedPerformanceInfo_Stock import Stock
from PerformanceAnalysis.AdvancedPerformanceInfo_Bond import pureBond, convertableBond


class DGAPI(object):
    def __init__(self, productID, EndDate=None):
        m = APIS()
        sqls = m.setAPIS(productID, EndDate)
        sqls_stock = m.stock(productID, EndDate)
        sqls_purebond = m.pureBond(productID, EndDate)
        sqls_convertablebond = m.convertableBond(productID, EndDate)
        ms = MySQLConnector()
        connection = ms.getConn()
        self.df = pd.read_sql(sql=sqls, con=connection)
        self.df_stock = pd.read_sql(sql=sqls_stock, con=connection)
        self.df_purebond = pd.read_sql(sql=sqls_purebond, con=connection)
        self.df_convertablebond = pd.read_sql(sql=sqls_convertablebond, con=connection)
        ms.closeConn()
        # print(self.df)

        td = TradingDay()
        self.checker_naturaldays = td.getProductNaturalDayCounts(productID, EndDate)
        self.checker_tradedays = td.getProductTradingDayCounts(productID, EndDate)


    def annualized_return(self):   # TODO: trade days and natural days need to calculate based on wind data
        # add check logic for trade/natural day calculation
        naturalday = pd.period_range(self.df['Occur_Date'].iloc[0], self.df['Occur_Date'].iloc[-1], freq='D')
        tradeday = len(self.df['Occur_Date'])

        print("Numbers of natural day: %d | checker: %d" % (naturalday.__len__(), self.checker_naturaldays))
        print("Numbers of trading day: %d | checker: %d" % (tradeday, self.checker_tradedays))
        navdelta = self.df['Accumulated_NAV'].iloc[-1].astype(float) - 1
        an = navdelta * 365 / naturalday.__len__()
        print("Annualized return: %.4f" % an)
        ar_list = [naturalday.__len__(), tradeday, round(an, 4)]
        return ar_list

    def leverage(self):
        lg = self.df["TotalAsset"].iloc[-1] / self.df["NAV"].iloc[-1]
        print("Leverage: %.4f" % lg)
        return round(lg, 4)

    def volatility(self):
        vol = self.df['Daily_Return'].std() * np.sqrt(250)
        if math.isnan(vol):
            vol = 0
        print("Volatility: %.4f" % vol)
        return round(vol, 4)

    def max_drawdown(self):
        self.df['max2here'] = pd.expanding_max(self.df['Accumulated_NAV'])
        self.df['dd2here'] = self.df['Accumulated_NAV'] / self.df['max2here'] - 1

        temp = self.df.sort_values(by='dd2here').iloc[0][['Occur_Date', 'dd2here']]
        max_dd = temp['dd2here']
        end_date = temp['Occur_Date']

        df2 = self.df[self.df['Occur_Date'] <= end_date]
        start_date = df2.sort_values(by='Accumulated_NAV', ascending=False).iloc[0]['Occur_Date']

        print("Max Drawdown: %.4f" % max_dd)
        print("Max Drawdown start from %s to %s" % (start_date, end_date))
        max_text = "Max Drawdown from %s to %s" % (start_date, end_date)
        md_list = [round(max_dd, 4), max_text]
        return md_list

    def sharpe(self):
        dailyReturn_mean = self.df['Daily_Return'].mean()
        dailyReturn_std = self.df['Daily_Return'].std()
        if dailyReturn_std == 0:
            sharpe = 0
        else:
            sharpe = (dailyReturn_mean / dailyReturn_std) * np.sqrt(250)
        if math.isnan(sharpe):
            sharpe = 0
        print("Sharpe: %.4f" % sharpe)
        return round(sharpe, 4)

    def stockPortfolioVolatility(self):
        # print(self.df_stock)
        if self.df_stock.empty:
            vol = 0
            print("Stock Portfolio Volatility: %.4f" % vol)
        else:
            s = Stock(self.df_stock)
            vol = s.portfolioVolatility()
        return round(vol, 4)

    def purebondPortfolioDuration(self):
        if self.df_purebond.empty:
            duration = 0
            duration_yield = 0
            purebond_vol = 0
            print("Portfolio Duration (pure bond): %.4f" % duration)
            print("Portfolio Yield Duration (pure bond): %.4f" % duration_yield)
            print("Pure Bond Volatility : %.4f" % purebond_vol)
            pb_list = [round(duration,4), round(duration_yield, 4), round(purebond_vol, 4)]
        else:
            s = pureBond(self.df_purebond)
            # d = s.duration().split(",")
            d = s.duration()
            v = s.portfolioVolatility()
            pb_list = [d[0], d[1], v]
        return pb_list

    def convertablebondVolatility(self):
        if self.df_convertablebond.empty:
            convertablebond_vol = 0
            print("Convertable Bond Volatility : %.4f" % convertablebond_vol)
        else:
            s = convertableBond(self.df_convertablebond)
            convertablebond_vol = s.portfolioVolatility()
        return round(convertablebond_vol, 4)


class DGAPI_insert(object):
    def __init__(self, productID, EndDate=None):
        self.productID = productID
        if EndDate is None:
            td = TradingDay()
            self.EndDate = td.getLastTradingDay()
        self.EndDate = EndDate

    def insertArray(self):
        # Compose Array
        dgapi = DGAPI(self.productID, self.EndDate)
        ar_list = dgapi.annualized_return()
        lg = dgapi.leverage()
        vl = dgapi.volatility()
        md_list = dgapi.max_drawdown()
        sp = dgapi.sharpe()
        svol = dgapi.stockPortfolioVolatility()
        cbvol = dgapi.convertablebondVolatility()
        pbd = dgapi.purebondPortfolioDuration()
        if pbd[0] == 0 or pbd[0] is None or pbd[1] == 0 or pbd[1] is None:
            dv01 = 0
        else:
            dv01 = float(pbd[0]) * float(lg)
        # dv01 = "%.4f" % dv01

        # AIPS_daily_data = ["'%s','%s',%d,%d,%.4f,%.4f,%.4f,%.4f,'%s',%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f"
        AIPS_daily_data = (self.EndDate,        # occur date
                           self.productID,      # product id
                           ar_list[0],         # natural days
                           ar_list[1],         # trading days
                           ar_list[2],         # annualized return
                           lg,                  # leverage
                           vl,                  # volatility
                           md_list[0],         # max draw down
                           md_list[1],         # max draw down period
                           sp,                  # sharpe
                           svol,                # stock portfolio volatility
                           cbvol,               # convertable bond portfolio volatility
                           pbd[2],              # pure bond volatility
                           pbd[0],              # pure bond duration (exercise)
                           pbd[1],              # pure bond duration (mature)
                           round(dv01, 4))               # dv01
        print(AIPS_daily_data)
        # print(type(AIPS_daily_data))

        # format
        insert_values = AIPS_daily_data

        # start mysql connection
        ms = MySQLConnector()
        msc = ms.getConn()
        cursor = msc.cursor()

        # compile sql statement
        insert_statement = """insert into Advanced_Performance_Info values('%s','%s',%d,%d,%f,%f,%f,%f,'%s',%f,%f,%f,%f,%f,%f,%f)""" % insert_values

        insert_result = cursor.execute(insert_statement)
        msc.commit()
        if insert_result:
            print("[Success] Insert completed!")
        else:
            print("[Error] Insert failed!")
        cursor.close()
        ms.closeConn()

if __name__ == '__main__':

    # gc = GlobalConfig()
    # productlist = gc.getConfig('product', 'list')
    # productlist = productlist.split(", ")
    p = 'FB0003'
    enddate = '20190118'
    # test = DGAPI_insert(p, enddate)
    # test.insertArray()


    # for p in productlist:
    #     print("========================================")
    #     print("Product ID : %s" % p)
    #     print("Static based on the data of : %s" % enddate)
    #     dgapi = DGAPI(p, enddate)
    #     dgapi.annualized_return()
    #     y = dgapi.leverage()
    #     dgapi.volatility()
    #     dgapi.max_drawdown()
    #     dgapi.sharpe()
    #     dgapi.stockPortfolioVolatility()
    #     dgapi.convertablebondVolatility()
    #     x = dgapi.purebondPortfolioDuration()
    #     if x[0] == 0 or x[0] is None or x[1] == 0 or x[1] is None:
    #         dv01 = 0
    #     else:
    #         dv01 = float(x[0]) * float(y)
    #     print("DV01 (pure bond): %.4f" % dv01)
    #     print("========================================")

    print("========================================")
    print("Product ID : %s" % p)
    print("Static based on the data of : %s" % enddate)
    dgapi = DGAPI(p, enddate)
    dgapi.annualized_return()
    y = dgapi.leverage()
    dgapi.volatility()
    dgapi.max_drawdown()
    dgapi.sharpe()
    dgapi.stockPortfolioVolatility()
    dgapi.convertablebondVolatility()
    x = dgapi.purebondPortfolioDuration()
    print(x)
    if x[0] == 0 or x[0] is None or x[1] == 0 or x[1] is None:
        dv01 = 0
    else:
        dv01 = float(x[0]) * float(y)
    print("DV01 (pure bond): %.4f" % dv01)
    print("========================================")


