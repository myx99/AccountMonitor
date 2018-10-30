from Common.MySQLConnector import MySQLConnector
from SQLStatement.BPI_select import BPIS


class DGBPI(object):
    def __init__(self):
        self.bpis = BPIS()
        datatype_tmp = self.bpis.getConfig('BPI_table', 'columns')
        self.datatype = datatype_tmp.split(", ")
        # print(self.datatype)
        self.BIPS_daily_data = []
        self.ms = MySQLConnector()
        self.msc = self.ms.getConn()
        self.cursor = self.msc.cursor()

    def composeArray(self, pid, bizdate):
        self.BIPS_daily_data = [bizdate, pid]

        # ms = MySQLConnector()
        # msc = ms.getConn()
        # cursor = msc.cursor()

        for dt in self.datatype:
            sqls = self.bpis.setBPIS(pid, bizdate, dt)
            # print(sqls)
            self.cursor.execute(sqls)
            value = self.cursor.fetchone()
            # print(value)
            if value is not None:
                append_value = float(value[0])
            else:
                append_value = 0
            # print(append_value)
            self.BIPS_daily_data.append(append_value)
        print(self.BIPS_daily_data)
        # cursor.close()
        # ms.closeConn()
        # return self.BIPS_daily_data

    def insertArray(self):
        # format list to tuple
        insert_values = tuple(self.BIPS_daily_data)

        # start mysql connection
        # ms = MySQLConnector()
        # msc = ms.getConn()
        # cursor = msc.cursor()

        # compile sql statement
        insert_statement = """insert into Basic_Performance_Info values('%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f)""" % insert_values

        insert_result = self.cursor.execute(insert_statement)
        self.msc.commit()
        if insert_result:
            print("[Success] Insert completed!")
        else:
            print("[Error] Insert failed!")
        self.cursor.close()
        self.ms.closeConn()


if __name__ == '__main__':
    pid = "FB0001"
    bizdate = '2017-11-13'
    dgbpi = DGBPI()
    dgbpi.composeArray(pid, bizdate)
    dgbpi.insertArray()
