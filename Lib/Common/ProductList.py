import Common.MySQLConnector as ms
import pandas as pd

import Lib.Common.GlobalConfig as cf


class ProductList(object):

    def __init__(self):
        config = cf.GlobalConfig()
        self.table = config.getConfig('mysql_tables', 'product')
        self.sqlStatement = "select * from %s" % self.table
        # print(self.sqlStatement)
        self.df = pd.DataFrame()
        self.mysql = ms.MySQLConnection()
        self.conn = self.mysql.getConn()

    def getProductTable(self):
        self.df = pd.read_sql(sql=self.sqlStatement, con=self.conn)
        self.mysql.closeConn()
        return self.df


if __name__ == '__main__':
    x = ProductList()
    y = x.getProductTable()
    print(y)
    print(y['ID'].values)
