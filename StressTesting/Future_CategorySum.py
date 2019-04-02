from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from StressTesting.FutureFilter import FutureFilter
from Common.GlobalConfig import GlobalConfig
import configparser
from Common.TradingDay import TradingDay
import numpy as np
import matplotlib.pyplot as plt


class Future_CategorySum(object):
    def __init__(self):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'StressTesting_win')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')
        td = TradingDay()
        self.ltd = td.getLastTradingDay()
        print("---- %s has been initialized! ----" % self.__class__.__name__)

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def Category_future(self, df):
        if df.empty:
            return None

        ms = MySQLConnector()
        mysql_connection = ms.getConn()
        oc = OracleConnector()
        oracle_connection = oc.getConn()

        futurelist = df['VC_SCDM'].values
        enddate = df['D_YWRQ'].iloc[0]
        enddate = enddate.strftime("%Y%m%d")

        sqls_td_days_Ashare = """select TRADE_DAYS "TRADE_DT" from
                                  (select distinct * from wind.CFuturesCalendar where s_info_exchmarket = 'CFFEX' and TRADE_DAYS <= '%s'order by TRADE_DAYS desc)  where rownum <= 120"""\
                                    % (enddate)
        df_120td_future = pd.read_sql(sql=sqls_td_days_Ashare, con=oracle_connection)
        df_120td_future['portfolio'] = float(0)
        startdate = df_120td_future['TRADE_DT'].values[-1]
        print("start date: %s | end date: %s" % (startdate, enddate))

        wind_code_list = []
        main_contract_code_list = []
        multiplier_list = []
        for f in futurelist:
            # get wind code and main contract
            sqls_code = """select S_INFO_WINDCODE "windcode", FS_INFO_SCCODE||substr(S_INFO_WINDCODE, -4) "maincontract" from wind.CFuturesDescription where S_INFO_CODE = '%s'""" % f
            df_code = pd.read_sql(sql=sqls_code, con=oracle_connection)
            wind_code = df_code.iat[0, 0]
            main_contract_code = df_code.iat[0, 1]
            # print(wind_code, main_contract_code)
            wind_code_list.append(wind_code)
            main_contract_code_list.append(main_contract_code)

            # get multiplier
            sqls_multiplier = """select case when s_info_cemultiplier is not null then s_info_cemultiplier else s_info_punit end from wind.CFuturescontpro where S_INFO_WINDCODE = '%s'""" % wind_code
            df_multiplier = pd.read_sql(sql=sqls_multiplier, con=oracle_connection)
            multiplier = df_multiplier.iat[0 ,0]
            multiplier_list.append(multiplier)

        df['WindCode'] = wind_code_list
        df['MainContract'] = main_contract_code_list
        df['Multiplier'] = multiplier_list
        # print(df)

        for index, row in df.iterrows():
            # get market data of main contract
            main_contract = row['MainContract']
            if main_contract[-4:] == '.CFE':
                if main_contract[0] == 'T':
                    sqls_future = """select TRADE_DT, S_DQ_SETTLE "%s" from wind.CBondFuturesEODPrices where S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s'and TRADE_DT <= '%s' order by TRADE_DT""" \
                                  % (main_contract, main_contract, startdate, enddate)
                else:
                    sqls_future = """select TRADE_DT, S_DQ_SETTLE "%s" from wind.CIndexFuturesEODPrices where S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s'and TRADE_DT <= '%s' order by TRADE_DT""" \
                       % (main_contract, main_contract, startdate, enddate)
            else:
                sqls_future = """select TRADE_DT, S_DQ_SETTLE "%s" from wind.CCommodityFuturesEODPrices where S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s'and TRADE_DT <= '%s' order by TRADE_DT""" \
                              % (main_contract, main_contract, startdate, enddate)
            # print(sqls_future)
            df_future = pd.read_sql(sql=sqls_future, con=oracle_connection)

            quantity = row['L_SL']
            marketvalue = row['EN_SZ']
            multiplier = row['Multiplier']

            if float(marketvalue) > 0:
                longorshort = 1
            elif float(marketvalue) < 0:
                longorshort = -1
            df_future[main_contract] = df_future[main_contract] * multiplier * quantity * longorshort
            df_120td_future = pd.merge(df_120td_future, df_future, how='outer', on='TRADE_DT')
            df_120td_future = df_120td_future.fillna(0)
            df_120td_future['portfolio'] += df_120td_future[main_contract]

        print(df)
        print(df_120td_future)

        df_120td_future['profit'] = df_120td_future['portfolio'].shift(1) - df_120td_future['portfolio']
        # print(df_120td_future)
        # writer = pd.ExcelWriter('D:\\PerformanceAnalysis\\futurevar.xlsx')
        # df_120td_future.to_excel(writer, sheet_name='Sheet1', index=False)
        profit = df_120td_future.loc[1:, 'profit']
        # print(profit)
        var_95 = np.percentile(profit, 5)
        var_98 = np.percentile(profit, 2)
        var_99 = np.percentile(profit, 1)
        print("95 var: %.4f" % var_95)
        print("98 var: %.4f" % var_98)
        print("99 var: %.4f" % var_99)

        df_temp = pd.DataFrame()
        df_temp['date'] = df_120td_future.loc[1:, 'TRADE_DT']
        df_temp['profit'] = df_120td_future.loc[1:, 'profit']
        print(df_temp)
        df_temp.plot(kind='line', grid='on', figsize=(20, 10), title="future portfolio")
        plt.show()

        return "finished"

if __name__ == '__main__':
    product = "FB0001"
    date = "20190211"
    ff = FutureFilter(product, date)
    futuresqls = ff.get_sqls()
    # print(futuresqls)
    ms = MySQLConnector()
    ms_connection = ms.getConn()
    df_future = pd.read_sql(sql=futuresqls, con=ms_connection)
    # print(df_future)
    fcs = Future_CategorySum()
    fcs_result = fcs.Category_future(df_future)
    print(fcs_result)
