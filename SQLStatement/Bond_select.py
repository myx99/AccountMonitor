import configparser
from Common.GlobalConfig import GlobalConfig


class Bond(object):

    def __init__(self):
        gc = GlobalConfig()
        path = gc.getConfig('SubConfigPath', 'BPI_conf_win')
        # print(path)
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def setBPIS(self, productID, date, datatype):
        table = self.getConfig('Valuation_Table_Info', 'table')
        column_date = self.getConfig("Valuation_Table_Info", "column_date")
        column_productid = self.getConfig("Valuation_Table_Info", "column_productid")
        target_column = self.getConfig(datatype, 'target_column')
        conditioning = self.getConfig(datatype, 'conditioning')
        condition = self.getConfig(datatype, 'condition')
        pattern_column = self.getConfig(datatype, 'pattern_column')
        target_column_alias = self.getConfig(datatype, 'target_column_alias')

        self.select_statement = "select %s as %s from %s " \
                           "where %s = '%s' " \
                           "and left(%s,10) =  '%s' " \
                           "and %s %s '%s' " \
                           % (target_column, target_column_alias, table,
                              column_productid, productID,
                              column_date, date,
                              pattern_column, conditioning, condition)
        return self.select_statement

if __name__ == '__main__':
    input_productid = 'FB0009'
    input_date = '2018-09-21'
    datatype = 'Equity_NAV_Percentage'
    m = BPIS()
    print(m.setBPIS(input_productid, input_date, datatype))
