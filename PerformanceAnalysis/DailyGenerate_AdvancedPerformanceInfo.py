import pandas as pd
from Common.MySQLConnector import MySQLConnector
from SQLStatement.API_select import APIS
import numpy as np
from Common.GlobalConfig import GlobalConfig
from PerformanceAnalysis.AdvancedPerformanceInfo_Stock import Stock


class DGAPI(object):
    def __init__(self, productID, EndDate=None):
        m = APIS()
        sqls = m.setAPIS(productID, EndDate)
        sqls_stock = m.stock(productID, EndDate)
        ms = MySQLConnector()
        connection = ms.getConn()
        self.df = pd.read_sql(sql=sqls, con=connection)
        self.df_stock = pd.read_sql(sql=sqls_stock, con=connection)
        ms.closeConn()

    def annualized_return(self):
        naturalday = pd.period_range(self.df['Occur_Date'].iloc[0], self.df['Occur_Date'].iloc[-1], freq='D')
        tradeday = len(self.df['Occur_Date'])
        print("Numbers of natural day: %d" % naturalday.__len__())
        print("Numbers of trading day: %d" % tradeday)
        navdelta = self.df['Accumulated_NAV'].iloc[-1].astype(float) - 1
        an = navdelta * 365 / naturalday.__len__()
        print("Annualized return: %.4f" % an)
        return "%.4f" % an

    def leverage(self):
        lg = self.df["TotalAsset"].iloc[-1] / self.df["NAV"].iloc[-1]
        print("Leverage: %.4f" % lg)
        return "%.4f" % lg

    def volatility(self):
        vol = self.df['Daily_Return'].std() * np.sqrt(250)
        print("Volatility: %.4f" % vol)
        return "%.4f" % vol

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
        return "%.4f" % max_dd

    def sharpe(self):
        dailyReturn_mean = self.df['Daily_Return'].mean()
        # nav_mean_daily = self.df['NAV'].mean()   #TODO: use the latest nav number as E(p)
        # nav_latest = self.df['NAV'].values[-1]
        # print(nav_latest)

        dailyReturn_std_daily = self.df['Daily_Return'].std()

        # sharp = (nav_mean_daily / nav_std_daily) * np.sqrt(250)
        sharpe = (dailyReturn_mean / dailyReturn_std_daily) * np.sqrt(250)
        print("Sharpe: %.4f" % sharpe)
        return "%.4f" % sharpe

    def stockPortfolioVolatility(self):
        # print(df)
        s = Stock(self.df_stock)
        s.portfolioVolatility()


if __name__ == '__main__':
    gc = GlobalConfig()
    productlist = gc.getConfig('product', 'list')
    productlist = productlist.split(", ")
    # productid = 'FB0003'
    enddate = '20181130'
    for p in productlist:
        print("Product ID : %s" % p)
        print("Static based on the data of : %s" % enddate)
        dgapi = DGAPI(p,enddate)
        dgapi.annualized_return()
        dgapi.leverage()
        dgapi.volatility()
        dgapi.max_drawdown()
        dgapi.sharpe()
        dgapi.stockPortfolioVolatility()


