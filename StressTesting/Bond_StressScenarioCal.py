from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from StressTesting.Bond_CategorySum import Bond_CategorySum
from StressTesting.BondFilter import BondFilter
from StressTesting.StressScenario import StressScenario
from Common.GlobalConfig import GlobalConfig
import configparser
from Common.TradingDay import TradingDay


class Bond_StressCalculate(object):
    def __init__(self):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'StressTesting_win')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')
        td = TradingDay()
        self.ltd = td.getLastTradingDay()

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    # 信用债违约损失
    def PureBondBreak(self, df):
        if df is None:
            return None

        ss = StressScenario()
        df_data = ss.ScenarioDataRetrieve('ScenarioList_Bond')
        mildlist = []
        moderatelist = []
        severelist = []
        for index, row in df.iterrows():
            # print(row['Rate'])
            if row['Rate'] == 'AAA':
                mild = float(df_data.loc[df_data['Scenario'] == 'AAA_bond_break', 'Mild']) * float(row['ValueSum'])
                moderate = float(df_data.loc[df_data['Scenario'] == 'AAA_bond_break', 'Moderate']) * float(row['ValueSum'])
                severe = float(df_data.loc[df_data['Scenario'] == 'AAA_bond_break', 'Severe']) * float(row['ValueSum'])
            elif row['Rate'] == 'AA+':
                mild = float(df_data.loc[df_data['Scenario'] == 'AAplus_bond_break', 'Mild']) * float(row['ValueSum'])
                moderate = float(df_data.loc[df_data['Scenario'] == 'AAplus_bond_break', 'Moderate']) * float(row['ValueSum'])
                severe = float(df_data.loc[df_data['Scenario'] == 'AAplus_bond_break', 'Severe']) * float(row['ValueSum'])
            else:
                mild = float(df_data.loc[df_data['Scenario'] == 'AA_andOther_bond_break', 'Mild']) * float(row['ValueSum'])
                moderate = float(df_data.loc[df_data['Scenario'] == 'AA_andOther_bond_break', 'Moderate']) * float(row['ValueSum'])
                severe = float(df_data.loc[df_data['Scenario'] == 'AA_andOther_bond_break', 'Severe']) * float(row['ValueSum'])

            mildlist.append(mild)
            moderatelist.append(moderate)
            severelist.append(severe)

        df['Mild'] = mildlist
        df['Moderate'] = moderatelist
        df['Severe'] = severelist
        # print(df)

        bond_break_result = ['PureBondBreak']
        list = ['Mild', 'Moderate', 'Severe']
        for i in list:
            bond_break_result.append(round(df[i].sum(), 2))
        return bond_break_result

    # 信用债下跌损失
    def PureBondLoss(self, df):
        if df is None:
            return None

        ss = StressScenario()
        df_data = ss.ScenarioDataRetrieve('ScenarioList_Bond')
        mildlist = []
        moderatelist = []
        severelist = []
        for index, row in df.iterrows():
            # print(row['Rate'])
            if row['Rate'] == 'AAA':
                mild = float(df_data.loc[df_data['Scenario'] == 'AAA_bond_loss', 'Mild']) * float(row['ValueSum']) * float(row['RatingDuration']) * (1 - float(df_data.loc[df_data['Scenario'] == 'AAA_bond_break', 'Mild']))
                moderate = float(df_data.loc[df_data['Scenario'] == 'AAA_bond_loss', 'Moderate']) * float(row['ValueSum']) * float(row['RatingDuration']) * (1 - float(df_data.loc[df_data['Scenario'] == 'AAA_bond_break', 'Moderate']))
                severe = float(df_data.loc[df_data['Scenario'] == 'AAA_bond_loss', 'Severe']) * float(row['ValueSum']) * float(row['RatingDuration']) * (1 - float(df_data.loc[df_data['Scenario'] == 'AAA_bond_break', 'Severe']))
            elif row['Rate'] == 'AA+':
                mild = float(df_data.loc[df_data['Scenario'] == 'AAplus_bond_loss', 'Mild']) * float(row['ValueSum']) * float(row['RatingDuration']) * (1 - float(df_data.loc[df_data['Scenario'] == 'AAplus_bond_break', 'Mild']))
                moderate = float(df_data.loc[df_data['Scenario'] == 'AAplus_bond_loss', 'Moderate']) * float(row['ValueSum']) * float(row['RatingDuration']) * (1 - float(df_data.loc[df_data['Scenario'] == 'AAplus_bond_break', 'Moderate']))
                severe = float(df_data.loc[df_data['Scenario'] == 'AAplus_bond_loss', 'Severe']) * float(row['ValueSum']) * float(row['RatingDuration']) * (1 - float(df_data.loc[df_data['Scenario'] == 'AAplus_bond_break', 'Severe']))
            else:
                mild = float(df_data.loc[df_data['Scenario'] == 'AA_andOther_bond_loss', 'Mild']) * float(row['ValueSum']) * float(row['RatingDuration']) * (1 - float(df_data.loc[df_data['Scenario'] == 'AA_andOther_bond_break', 'Mild']))
                moderate = float(df_data.loc[df_data['Scenario'] == 'AA_andOther_bond_loss', 'Moderate']) * float(row['ValueSum']) * float(row['RatingDuration']) * (1 - float(df_data.loc[df_data['Scenario'] == 'AA_andOther_bond_break', 'Moderate']))
                severe = float(df_data.loc[df_data['Scenario'] == 'AA_andOther_bond_loss', 'Severe']) * float(row['ValueSum']) * float(row['RatingDuration']) * (1 - float(df_data.loc[df_data['Scenario'] == 'AA_andOther_bond_break', 'Severe']))

            mildlist.append(mild)
            moderatelist.append(moderate)
            severelist.append(severe)

        df['Mild'] = mildlist
        df['Moderate'] = moderatelist
        df['Severe'] = severelist
        # print(df)

        bond_loss_result = ['PureBondLoss']
        list = ['Mild', 'Moderate', 'Severe']
        for i in list:
            bond_loss_result.append(round(df[i].sum(), 2))
        return bond_loss_result

    def CBondLoss(self, df):
        if df is None:
            return None

        ss = StressScenario()
        df_data = ss.ScenarioDataRetrieve('ScenarioList_ConvertibleBond')
        df_temp = pd.merge(df, df_data, on='Scenario', how='left')
        df_temp['Mild'] = df_temp['value_by_index'].astype(float) * df_temp['beta_by_index'].astype(float) * df_temp['Mild'].astype(float)
        df_temp['Moderate'] = df_temp['value_by_index'].astype(float) * df_temp['beta_by_index'].astype(float) * df_temp['Moderate'].astype(float)
        df_temp['Severe'] = df_temp['value_by_index'].astype(float) * df_temp['beta_by_index'].astype(float) * df_temp['Severe'].astype(float)

        cb_result = ["ConvertibleBondLoss"]
        list = ['Mild', 'Moderate', 'Severe']
        for i in list:
            cb_result.append(round(df_temp[i].sum(), 2))
        return cb_result

if __name__ == '__main__':
    product = "FB0009"
    date = "20190211"
    bf = BondFilter(product, date)
    sqls_cb = bf.get_CBond_sqls()
    sqls_pb = bf.get_PBond_sqls()
    ms = MySQLConnector()
    mysql_connection = ms.getConn()
    df_cb = pd.read_sql(sql=sqls_cb, con=mysql_connection)
    df_pb = pd.read_sql(sql=sqls_pb, con=mysql_connection)

    bcs = Bond_CategorySum()
    cb = bcs.Category_CB(df_cb)
    pb = bcs.CreditCategory_PB(df_pb)

    bsc = Bond_StressCalculate()
    pbb = bsc.PureBondBreak(pb)
    pbl = bsc.PureBondLoss(pb)
    cbl = bsc.CBondLoss(cb)
    print(pbb)
    print(pbl)
    print(cbl)
