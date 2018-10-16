import pymysql

import Lib.Common.GlobalConfig as cf


class MySQLConnector(object):

    def __init__(self):
        # print("Setting configurations")
        config = cf.GlobalConfig()
        self.host = config.getConfig('mysql', 'host')
        self.port = config.getIntConfig('mysql', 'port')
        self.user = config.getConfig('mysql', 'user')
        self.passwd = config.getConfig('mysql', 'passwd')
        self.db = config.getConfig('mysql', 'db')
        self.use_unicode = config.getBooleanConfig('mysql', 'use_unicode')
        self.charset = config.getConfig('mysql', 'charset')
        # print("Initiation of MySQLConnection class is finished")

    def getConn(self):
        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd,
                                    db=self.db, use_unicode=self.use_unicode, charset=self.charset)
        # print("Connected to MySQL DB")
        return self.conn

    def closeConn(self):
        self.conn.close()
        # print("Closed connection with MySQL DB")


if __name__ == '__main__':
    m = MySQLConnector()
    m.getConn()
    m.closeConn()
