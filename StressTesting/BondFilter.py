import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd


class BondFilter(object):
    def __init__(self, productID, EndDate=None):
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'StressTesting_win')
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')
        td = TradingDay()
        self.ltd = td.getLastTradingDay()

        self.productID = productID
        self.EndDate = EndDate

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def prefix2sql(self, prefixlist):
        valuation_table = self.getConfig('Valuation_Table_Info', 'table')
        column_productid = self.getConfig('Valuation_Table_Info', 'column_productid')
        column_date = self.getConfig('Valuation_Table_Info', 'column_date')
        column_kmdm = self.getConfig('Valuation_Table_Info', 'column_KMDM')

        EndDate = self.EndDate

        if EndDate is None:
            EndDate = self.ltd
        td = "%s-%s-%s" % (EndDate[:4], EndDate[4:6], EndDate[-2:])

        bfc_List = []
        for bond in prefixlist:
            if bond != prefixlist[-1]:
                bfc = "%s like '%s_%%' or" % (column_kmdm, bond)
            else:
                bfc = "%s like '%s_%%'" % (column_kmdm, bond)
            bfc_List.append(bfc)
        BondFuzzyCondition = ' '.join(bfc_List)
        # print(bfc_List)
        # print(BondFuzzyCondition)
        sqls = "select distinct * from %s where %s = '%s' and left(%s,10) = '%s' and (%s)" \
                    % (valuation_table, column_productid, self.productID, column_date, td, BondFuzzyCondition)
        return sqls

    def get_PBond_sqls(self):
        PBondPrefixList = self.getConfig('BondCategory', 'PureBond_prefix')
        PBondPrefixList = PBondPrefixList.split(", ")

        PBondSqls = self.prefix2sql(PBondPrefixList)

        return PBondSqls

    def get_CBond_sqls(self):
        CBondPrefixList = self.getConfig('BondCategory', 'ConvertableBondPrefixList')
        CBondPrefixList = CBondPrefixList.split(", ")

        CBondSqls = self.prefix2sql(CBondPrefixList)

        return CBondSqls

if __name__ == '__main__':
    product = "FB0009"
    date = "20190211"
    m = BondFilter(product, date)
    pbondsqls = m.get_PBond_sqls()
    cbondsqls = m.get_CBond_sqls()
    ms = MySQLConnector()
    connection = ms.getConn()
    df_pb = pd.read_sql(sql=pbondsqls, con=connection)
    df_cb = pd.read_sql(sql=cbondsqls, con=connection)
    print(df_pb, df_cb)

