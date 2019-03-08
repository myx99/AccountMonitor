from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from StressTesting.StockFilter import StockFilter
from StressTesting.Stock_BetaCal import Stock_BetaCal
from StressTesting.StressScenario import StressScenario
from Common.GlobalConfig import GlobalConfig
import configparser
from Common.TradingDay import TradingDay


class Stock_StressCalculate(object):
    def __init__(self):
        td = TradingDay()
        self.ltd = td.getLastTradingDay()
        ss = StressScenario()
        self.scenario = ss.ScenarioDataRetrieve('ScenarioList_Stock')

    # 股票下跌损失
    def StockLoss(self, df):
        if df is None:
            return None
        df_data = self.scenario
        df.rename(columns={'Index': 'Scenario'}, inplace=True)
        df_temp = pd.merge(df, df_data, on='Scenario', how='left')
        df_temp['Mild'] = df_temp['value_by_index'].astype(float) * df_temp['beta_by_index'].astype(float) * df_temp['Mild'].astype(float)
        df_temp['Moderate'] = df_temp['value_by_index'].astype(float) * df_temp['beta_by_index'].astype(float) * df_temp['Moderate'].astype(float)
        df_temp['Severe'] = df_temp['value_by_index'].astype(float) * df_temp['beta_by_index'].astype(float) * df_temp['Severe'].astype(float)
        # print(df_temp)

        stock_result = ["StockLoss"]
        list = ['Mild', 'Moderate', 'Severe']
        for i in list:
            stock_result.append(round(df_temp[i].sum(), 2))
        return stock_result


if __name__ == '__main__':
    product = "FB0003"
    date = "20190211"
    m = StockFilter(product, date)
    e, x, y = m.getdf()
    n = Stock_BetaCal()
    z = n.Beta(e, x, y)
    o = Stock_StressCalculate()
    a = o.StockLoss(z)
    print(a)
