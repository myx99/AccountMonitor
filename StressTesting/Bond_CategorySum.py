from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from StressTesting.BondFilter import BondFilter
from Common.GlobalConfig import GlobalConfig
import configparser
from Common.TradingDay import TradingDay
import numpy as np


class Bond_CategorySum(object):
    def __init__(self):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'StressTesting_win')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')
        td = TradingDay()
        self.ltd = td.getLastTradingDay()

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def CreditCategory_PB(self, df):
        if df.empty:
            return None

        oc = OracleConnector()
        oracle_connection = oc.getConn()

        df.loc[df['L_SCLB'] == 1, 'VC_SCDM'] += '.SH'
        df.loc[df['L_SCLB'] == 2, 'VC_SCDM'] += '.SZ'
        df.loc[df['L_SCLB'] == 3, 'VC_SCDM'] += '.IB'
        bondlist = df['VC_SCDM'].values
        enddate = df['D_YWRQ'].iloc[0]
        enddate = enddate.strftime("%Y%m%d")
        # print(bondlist)

        bond_modidura_list = []
        bond_modidura_list_yield = []
        ratingcollector = []
        for b in bondlist:
            # ------   get rating   -------#
            sqls = """select b_info_creditrating from (select * from wind.CBondRating where S_INFO_WINDCODE = '%s' order by ANN_DT DESC) where rownum =1""" % b
            # print(sqls)
            dfx = pd.read_sql(sql=sqls, con=oracle_connection)
            # print(dfx)
            if dfx.empty:
                sqls1 = """select b_info_creditrating from (select * from wind.Cbondissuerrating
                          where s_info_compname = (select b_info_issuer from wind.cbonddescription
                          where S_INFO_WINDCODE = '%s')order by ANN_DT desc) where rownum=1""" % b
                # print(sqls1)
                dfx1 = pd.read_sql(sql=sqls1, con=oracle_connection)
                # print(dfx1)
                if dfx1.empty:
                    ratingunit = 'NoRating'
                else:
                    ratingunit = dfx1['B_INFO_CREDITRATING'].values[0]
            else:
                ratingunit = dfx['B_INFO_CREDITRATING'].values[0]
            ratingcollector.append(ratingunit)

            # ------   get duration   -------#
            sqlsd = """select b_anal_modidura_cnbd from wind.cbondanalysiscnbd
                               where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s'
                               and B_ANAL_YIELD_CNBD = (select min(B_ANAL_YIELD_CNBD) from wind.cbondanalysiscnbd where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s')""" \
                   % (b, enddate, b, enddate)
            sqlsd_yield = """select b_anal_modidura_cnbd from wind.cbondanalysiscnbd
                               where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s'
                               and B_ANAL_YIELD_CNBD = (select max(B_ANAL_YIELD_CNBD) from wind.cbondanalysiscnbd where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s')""" \
                         % (b, enddate, b, enddate)
            bond_modidura_df = pd.read_sql(sql=sqlsd, con=oracle_connection)
            bond_modidura_df_yield = pd.read_sql(sql=sqlsd_yield, con=oracle_connection)
            # print(purebond_modidura_df)
            bond_modidura = bond_modidura_df['B_ANAL_MODIDURA_CNBD'].values[0]
            bond_modidura_yield = bond_modidura_df_yield['B_ANAL_MODIDURA_CNBD'].values[0]
            # print(purebond_modidura)
            bond_modidura_list.append(bond_modidura)
            bond_modidura_list_yield.append(bond_modidura_yield)

        oc.closeConn()
        # ------   collect rating   -------#
        # print(ratingcollector)
        df['Rating'] = ratingcollector
        # CategoryResult = df.groupby(by=['Rating'])['EN_SZ'].sum()
        CategoryResult = df.groupby('Rating')['EN_SZ'].sum()
        # print(CategoryResult)
        ratinglist = list(CategoryResult.index)
        # print(ratinglist)
        valuesumlist = list(CategoryResult)
        # print(valuesumlist)
        df_credit_sum_duration = pd.DataFrame()
        df_credit_sum_duration['Rate'] = ratinglist
        df_credit_sum_duration['ValueSum'] = valuesumlist
        # print(df_credit_sum_duration)

        # ------   duration by rating   -------#
        df['Duration_E'] = bond_modidura_list
        df['Duration_M'] = bond_modidura_list_yield
        ratingdurationcollector = []
        for r in ratinglist:
            # print(r)
            df_temp = df.loc[df['Rating'] == r]
            # print(df_temp)
            df_temp['purebond_percentage'] = df_temp['EN_SZ'] / df_temp['EN_SZ'].sum()
            CreditCategoryDuration = (df_temp['Duration_E'] * df_temp['purebond_percentage']).sum()
            # print(CreditCategoryDuration)
            ratingdurationcollector.append(CreditCategoryDuration)
        df_credit_sum_duration['RatingDuration'] = ratingdurationcollector
        # print(df_credit_sum_duration)

        return df_credit_sum_duration

    def Category_CB(self, df):
        if df.empty:
            return None

        ms = MySQLConnector()
        mysql_connection = ms.getConn()
        oc = OracleConnector()
        oracle_connection = oc.getConn()

        df.loc[df['L_SCLB'] == 1, 'VC_SCDM'] += '.SH'
        df.loc[df['L_SCLB'] == 2, 'VC_SCDM'] += '.SZ'
        df.loc[df['L_SCLB'] == 3, 'VC_SCDM'] += '.IB'
        bondlist = df['VC_SCDM'].values
        enddate = df['D_YWRQ'].iloc[0]
        enddate = enddate.strftime("%Y%m%d")

        sqls_td_days_Ashare = """select TRADE_DAYS "TRADE_DT" from
                                  (select distinct * from wind.AShareCalendar where S_INFO_EXCHMARKET = 'SZSE' and TRADE_DAYS <= '%s'order by TRADE_DAYS desc)  where rownum <= 120"""\
                                    % (enddate)
        df_120td_Ashare = pd.read_sql(sql=sqls_td_days_Ashare, con=oracle_connection)
        startdate = df_120td_Ashare['TRADE_DT'].values[-1]

        cb_index = '000832.CSI'
        sqls_index = """select distinct TRADE_DT, S_DQ_PCTCHANGE "%s" from wind.AIndexEODPrices where S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s'and TRADE_DT <= '%s' order by TRADE_DT""" \
               % (cb_index, cb_index, startdate, enddate)
        df_index = pd.read_sql(sql=sqls_index, con=oracle_connection)
        beta_list = []
        for b in bondlist:
            sqls = """select distinct TRADE_DT, S_DQ_PCTCHANGE "%s" from wind.CBondEODPrices where S_INFO_WINDCODE = '%s' and TRADE_DT >= '%s' and TRADE_DT <= '%s' order by TRADE_DT""" \
                   % (b, b, startdate, enddate)
            df_cbond = pd.read_sql(sql=sqls, con=oracle_connection)

            sqls_suspend = """select distinct * from wind.CBondEODPrices where  S_INFO_WINDCODE = '%s'and TRADE_DT >= '%s' and TRADE_DT <= '%s' and S_DQ_TRADESTATUS = '停牌'""" \
                           % (b, startdate, enddate)
            df_cbond_suspend = pd.read_sql(sql=sqls_suspend, con=oracle_connection)

            if len(df_cbond) != 120:
                beta = 1
            elif not df_cbond_suspend.empty:
                beta = 1
            else:
                beta = df_cbond[b].cov(df_index[cb_index]) / df_index[cb_index].var()
            beta_list.append(beta)
        df['Beta'] = beta_list
        df['Beta_by_value'] = df['Beta'] * df['EN_SZ']
        # print(df)

        df_result = pd.DataFrame()
        Scenario_list = ["ConvertibleBond_loss"]
        ValueSum_list = []
        BetaSum_list = []
        value_by_index = df['EN_SZ'].sum()
        beta_by_index = np.average(df['Beta'], weights=df['EN_SZ'])
        ValueSum_list.append(value_by_index)
        BetaSum_list.append(beta_by_index)
        df_result['Scenario'] = Scenario_list
        df_result['value_by_index'] = ValueSum_list
        df_result['beta_by_index'] = BetaSum_list

        return df_result


if __name__ == '__main__':
    product = "FB0009"
    date = "20190211"
    m = BondFilter(product, date)
    sqls_cb = m.get_CBond_sqls()
    sqls_pb = m.get_PBond_sqls()
    ms = MySQLConnector()
    mysql_connection = ms.getConn()
    df_cb = pd.read_sql(sql=sqls_cb, con=mysql_connection)
    df_pb = pd.read_sql(sql=sqls_pb, con=mysql_connection)

    m = Bond_CategorySum()
    cb = m.Category_CB(df_cb)
    pb = m.CreditCategory_PB(df_pb)
    print(pb)
    print(cb)
