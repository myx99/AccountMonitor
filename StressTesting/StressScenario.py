from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd
from Common.GlobalConfig import GlobalConfig
import configparser
from Common.TradingDay import TradingDay


class StressScenario(object):
    def __init__(self):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'StressTesting_win')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')
        td = TradingDay()
        self.ltd = td.getLastTradingDay()

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def ScenarioDataRetrieve(self, typelist):
        df_Scenario = pd.DataFrame(columns=('Scenario', 'Mild', 'Moderate', 'Severe'))
        # print(df_Scenario)
        ScenarioList = self.getConfig('Scenario', typelist)
        ScenarioList = ScenarioList.split(", ")
        # print(ScenarioList)

        for sl in ScenarioList:
            # print(sl)
            sl_scenario = self.getConfig('Scenario', sl)
            sl_scenario = sl_scenario.split(", ")
            sl_mild = sl_scenario[0]
            sl_moderate = sl_scenario[1]
            sl_severe = sl_scenario[2]
            # print(sl_mild, sl_moderate, sl_severe)

            df_Scenario = df_Scenario.append({'Scenario': sl, 'Mild': sl_mild, 'Moderate': sl_moderate, 'Severe': sl_severe}, ignore_index=True)
            # print(df_Scenario)

        return df_Scenario


if __name__ == '__main__':
    m = StressScenario()
    x = m.ScenarioDataRetrieve('ScenarioList_Stock')
    y = m.ScenarioDataRetrieve('ScenarioList_Bond')
    z = m.ScenarioDataRetrieve('ScenarioList_ConvertibleBond')
    print(x)
    print("-------")
    print(y)
    print("-------")
    print(z)
