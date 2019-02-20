from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from StressTesting.Bond_CategorySum import Bond_CategorySum
from StressTesting.StressScenario import StressScenario
from Common.GlobalConfig import GlobalConfig
import configparser
from Common.TradingDay import TradingDay


class Bond_StressCalculate(object):
    def __init__(self):
        td = TradingDay()
        self.ltd = td.getLastTradingDay()
        ss = StressScenario()
        self.scenario = ss.ScenarioDataRetrieve()

    # 信用债违约损失
    def PureBondBreak(self, df):
        df_data = self.scenario
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
        return df

    # 信用债下跌损失
    def PureBondLoss(self, df):
        df_data = self.scenario
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
        return df


if __name__ == '__main__':
    product = "FB0006"
    date = "20190211"
    m = Bond_CategorySum(product, date)
    x = m.CreditCategory_PB()
    n = Bond_StressCalculate()
    y = n.PureBondBreak(x)
    print(y)
    z = n.PureBondLoss(x)
    print(z)
