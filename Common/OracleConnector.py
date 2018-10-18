import cx_Oracle   # cp D:\Oracle\product\11.2.0\client_1\BIN\oci.dll D:\Anaconda3\Lib\site-packages if failed
import pandas as pd
from Common.GlobalConfig import GlobalConfig


class OracleConnector(object):

    def __init__(self):
        # print("Setting configurations")
        config = GlobalConfig()
        self.host = config.getConfig('Oracle', 'host')
        self.user = config.getConfig('Oracle', 'user')
        self.passwd = config.getConfig('Oracle', 'passwd')
        self.db = config.getConfig('Oracle', 'db')

    def getConn(self):
        compose = self.user + '/' + self.passwd + '@' + self.host + '/' + self.db
        self.conn = cx_Oracle.connect(compose)
        return self.conn

    def closeConn(self):
        self.conn.close()


if __name__ == '__main__':
    m = OracleConnector()
    connection = m.getConn()
    sqls = "select trade_days from wind.asharecalendar where S_INFO_EXCHMARKET = 'SZSE' and trade_days >= '20170118' and trade_days <= '20181010' order by trade_days"
    df = pd.read_sql(sql=sqls, con=connection)
    print(df['TRADE_DAYS'])
    m.closeConn()
