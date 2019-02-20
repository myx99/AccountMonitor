from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from StressTesting.BondFilter import BondFilter
from Common.GlobalConfig import GlobalConfig
import configparser
from Common.TradingDay import TradingDay


class Bond_CategorySum(object):
    def __init__(self, product, Enddate=None):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'StressTesting_win')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')
        td = TradingDay()
        self.ltd = td.getLastTradingDay()
        self.product = product
        self.Enddate = Enddate

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def CreditCategory_PB(self):
        product = self.product
        Enddate = self.Enddate

        m = BondFilter(product, Enddate)
        sqls = m.get_PBond_sqls()
        ms = MySQLConnector()
        connection = ms.getConn()
        df = pd.read_sql(sql=sqls, con=connection)

        if df.empty:
            return None

        df.loc[df['L_SCLB'] == 1, 'VC_SCDM'] += '.SH'
        df.loc[df['L_SCLB'] == 2, 'VC_SCDM'] += '.SZ'
        df.loc[df['L_SCLB'] == 3, 'VC_SCDM'] += '.IB'

        bondlist = df['VC_SCDM'].values
        # print(bondlist)
        oc = OracleConnector()
        connect = oc.getConn()

        bond_modidura_list = []
        bond_modidura_list_yield = []
        ratingcollector = []
        for b in bondlist:
            # ------   get rating   -------#
            sqls = """select b_info_creditrating from (select * from wind.CBondRating where S_INFO_WINDCODE = '%s' order by ANN_DT DESC) where rownum =1""" % b
            # print(sqls)
            dfx = pd.read_sql(sql=sqls, con=connect)
            # print(dfx)
            if dfx.empty:
                sqls1 = """select b_info_creditrating from (select * from wind.Cbondissuerrating
                          where s_info_compname = (select b_info_issuer from wind.cbonddescription
                          where S_INFO_WINDCODE = '%s')order by ANN_DT desc) where rownum=1""" % b
                # print(sqls1)
                dfx1 = pd.read_sql(sql=sqls1, con=connect)
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
                   % (b, Enddate, b, Enddate)
            sqlsd_yield = """select b_anal_modidura_cnbd from wind.cbondanalysiscnbd
                               where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s'
                               and B_ANAL_YIELD_CNBD = (select max(B_ANAL_YIELD_CNBD) from wind.cbondanalysiscnbd where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s')""" \
                         % (b, Enddate, b, Enddate)
            bond_modidura_df = pd.read_sql(sql=sqlsd, con=connect)
            bond_modidura_df_yield = pd.read_sql(sql=sqlsd_yield, con=connect)
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


if __name__ == '__main__':
    product = "FB0006"
    date = "20190211"
    m = Bond_CategorySum(product, date)
    x = m.CreditCategory_PB()
    print(x)
