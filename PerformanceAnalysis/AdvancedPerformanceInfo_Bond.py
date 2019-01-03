import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from SQLStatement.API_select import APIS
import datetime
import numpy as np


class pureBond(object):
    def __init__(self, df):
        df.loc[df['L_SCLB'] == 1, 'VC_SCDM'] += '.SH'
        df.loc[df['L_SCLB'] == 2, 'VC_SCDM'] += '.SZ'
        df.loc[df['L_SCLB'] == 3, 'VC_SCDM'] += '.IB'  # TODO: need to verify when start to trade inter bank bond

        self.purebondlist = df['VC_SCDM'].values
        # print("Pure Bond List : %s" % self.purebondlist)

        enddate = df['D_YWRQ'].iloc[0]
        self.startdate = (enddate - datetime.timedelta(days=180)).strftime("%Y%m%d")
        self.startdate_minus1day = (enddate - datetime.timedelta(days=181)).strftime("%Y%m%d")
        self.enddate = enddate.strftime("%Y%m%d")
        self.enddate_minus1day = (enddate - datetime.timedelta(days=1)).strftime("%Y%m%d")
        print("purebond - start: %s | end: %s" % (self.startdate, self.enddate))
        print("PB vol - start_minus1day: %s | end_minus1day: %s | start: %s | end: %s" % (self.startdate_minus1day, self.enddate_minus1day, self.startdate, self.enddate))

        self.df_input = df

    def duration(self):
        oc = OracleConnector()
        oracle_connection = oc.getConn()
        purebond_modidura_list = []
        purebond_modidura_list_yield = []

        for purebond in self.purebondlist:
            sqls = """select b_anal_modidura_cnbd from wind.cbondanalysiscnbd
                       where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s'
                       and B_ANAL_YIELD_CNBD = (select min(B_ANAL_YIELD_CNBD) from wind.cbondanalysiscnbd where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s')""" \
                   % (purebond, self.enddate, purebond, self.enddate)
            sqls_yield = """select b_anal_modidura_cnbd from wind.cbondanalysiscnbd
                       where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s'
                       and B_ANAL_YIELD_CNBD = (select max(B_ANAL_YIELD_CNBD) from wind.cbondanalysiscnbd where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s')""" \
                         % (purebond, self.enddate, purebond, self.enddate)
            # print(sqls)
            purebond_modidura_df = pd.read_sql(sql=sqls, con=oracle_connection)
            purebond_modidura_df_yield = pd.read_sql(sql=sqls_yield, con=oracle_connection)
            # print(purebond_modidura_df)
            purebond_modidura = purebond_modidura_df['B_ANAL_MODIDURA_CNBD'].values[0]
            purebond_modidura_yield = purebond_modidura_df_yield['B_ANAL_MODIDURA_CNBD'].values[0]
            # print(purebond_modidura)
            purebond_modidura_list.append(purebond_modidura)
            purebond_modidura_list_yield.append(purebond_modidura_yield)
        oc.closeConn()

        df = self.df_input
        df['Modidura'] = purebond_modidura_list
        df['Modidura_yield'] = purebond_modidura_list_yield
        df['purebond_percentage'] = df['EN_SZ'] / df['EN_SZ'].sum()
        portfolioDuration = (df['Modidura'] * df['purebond_percentage']).sum()
        portfolioDuration_yield = (df['Modidura_yield'] * df['purebond_percentage']).sum()
        # print(self.df_input)

        print("Portfolio Duration (pure bond): %.4f" % portfolioDuration)
        print("Portfolio Yield Duration (pure bond): %.4f" % portfolioDuration_yield)
        duration_list = [round(portfolioDuration, 4), round(portfolioDuration_yield, 4)]
        return duration_list

    def portfolioVolatility(self):
        td = TradingDay()
        tradedays = td.getDuration(self.startdate, self.enddate)
        df_pcntg = pd.DataFrame()
        df_pcntg['TRADE_DT'] = tradedays
        df_pcntg['portfolio'] = float(0)
        # print(df_pcntg)

        oc = OracleConnector()
        oracle_connection = oc.getConn()
        ms = MySQLConnector()
        mysql_connection = ms.getConn()
        for pb in self.purebondlist:
            sqls = """select TRADE_DT, b_anal_net_cnbd "%s" from wind.cbondanalysiscnbd where S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s'and TRADE_DT <= '%s' order by TRADE_DT""" \
                   % (pb, pb, self.startdate, self.enddate)
            sqls_minus1day = """select TRADE_DT, b_anal_net_cnbd "%s" from wind.cbondanalysiscnbd where S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s'and TRADE_DT <= '%s' order by TRADE_DT""" \
                   % (pb, pb, self.startdate_minus1day, self.enddate_minus1day)
            # print(sqls)
            df_pb = pd.read_sql(sql=sqls, con=oracle_connection)
            df_pb_minus1day = pd.read_sql(sql=sqls_minus1day, con=oracle_connection)
            # print(df_pb, df_pb_minus1day)
            df_pb[pb] = (df_pb[pb] / df_pb_minus1day[pb] -1) * 100
            # print(df_pb)
            weight = self.df_input.loc[self.df_input['VC_SCDM'] == pb, 'EN_SZZJZ'].values
            # print(weight)
            df_pb[pb] = df_pb[pb].astype(float) * weight / 100
            # print(type(df_pb['TRADE_DT'][0]))
            df_pcntg = pd.merge(df_pcntg, df_pb, how='outer', on='TRADE_DT')
            # df_pcntg = df_pcntg.fillna(0)
            df_pcntg['portfolio'] += df_pcntg[pb]
            # print(df_pcntg)
        oc.closeConn()
        ms.closeConn()

        # print(list(df_pcntg['portfolio']))
        # writer = pd.ExcelWriter("D:\PerformanceAnalysis/output.xlsx")
        # df_pcntg.to_excel(writer, 'Sheet2')
        # writer.save()
        vol = df_pcntg['portfolio'].std() * np.sqrt(20)
        print("PB Portfolio Volatility: %.4f" % vol)
        return round(vol, 4)


class convertableBond(object):
    def __init__(self, df):
        df.loc[df['L_SCLB'] == 1, 'VC_SCDM'] += '.SH'
        df.loc[df['L_SCLB'] == 2, 'VC_SCDM'] += '.SZ'

        self.cblist = df['VC_SCDM'].values
        # print("CB List : %s" % self.cblist)

        enddate = df['D_YWRQ'].iloc[0]
        self.startdate = (enddate - datetime.timedelta(days=180)).strftime("%Y%m%d")
        self.enddate = enddate.strftime("%Y%m%d")
        print("CB vol - start: %s | end: %s" % (self.startdate, self.enddate))

        self.df_input = df

    def portfolioVolatility(self):
        td = TradingDay()
        tradedays = td.getDuration(self.startdate, self.enddate)
        df_pcntg = pd.DataFrame()
        df_pcntg['TRADE_DT'] = tradedays
        df_pcntg['portfolio'] = float(0)
        # print(df_pcntg)

        oc = OracleConnector()
        oracle_connection = oc.getConn()
        ms = MySQLConnector()
        mysql_connection = ms.getConn()
        for cb in self.cblist:
            sqls = """select TRADE_DT, S_DQ_PCTCHANGE "%s" from wind.CBondEODPrices where S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s'and TRADE_DT <= '%s' order by TRADE_DT""" \
                   % (cb, cb, self.startdate, self.enddate)
            # print(sqls)
            df_cb = pd.read_sql(sql=sqls, con=oracle_connection)
            weight = self.df_input.loc[self.df_input['VC_SCDM'] == cb, 'EN_SZZJZ'].values
            # print(weight)
            df_cb[cb] = df_cb[cb].astype(float) * weight / 100
            # print(type(df_cb['TRADE_DT'][0]))
            df_pcntg = pd.merge(df_pcntg, df_cb, how='outer', on='TRADE_DT')
            # df_pcntg = df_pcntg.fillna(0)
            df_pcntg['portfolio'] += df_pcntg[cb]
            # print(df_pcntg)
        oc.closeConn()
        ms.closeConn()

        # print(list(df_pcntg['portfolio']))
        # writer = pd.ExcelWriter("D:\PerformanceAnalysis/output.xlsx")
        # df_pcntg.to_excel(writer, 'Sheet2')
        # writer.save()
        vol = df_pcntg['portfolio'].std() * np.sqrt(20)
        print("CB Portfolio Volatility: %.4f" % vol)
        return round(vol, 4)


if __name__ == '__main__':
    # ---- pure bond ------- #
    p = "FB0006"
    m = APIS()
    sqls = m.pureBond(p, '20190102')
    ms = MySQLConnector()
    connection = ms.getConn()
    df = pd.read_sql(sql=sqls, con=connection)
    # print(df)
    s = pureBond(df)
    s.portfolioVolatility()
    # x = s.duration().split(",")
    # print(x[0], x[1])

    # ---- convertable bond ------- #
    # p = "FB0008"
    # m = APIS()
    # sqls = m.convertableBond(p,'20181023')
    # ms = MySQLConnector()
    # connection = ms.getConn()
    # df = pd.read_sql(sql=sqls, con=connection)
    # print(df)
    # s = convertableBond(df)
    # s.portfolioVolatility()
