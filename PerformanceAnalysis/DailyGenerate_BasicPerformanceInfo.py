from Common.MySQLConnector import MySQLConnector
from SQLStatement.BPI_select import BPIS


def composeArray(pid, bizdate):
    datatype = ['TotalAsset', 'NAV', 'Unit_NAV', 'Accumulated_NAV',
                'Daily_Return', 'MTD_Return', 'YTD_Return',
                'Equity_NAV_Percentage', 'Bond_NAV_percentage', 'Derivative_NAV_percentage', 'Fund_NAV_percentage']
    BIPS_daily_data = [bizdate, pid]
    b1 = BPIS()
    s1 = MySQLConnector()
    s2 = s1.getConn()
    cursor = s2.cursor()

    for dt in datatype:
        b2 = b1.setBPIS(pid, bizdate, dt)
        # print(b2)
        cursor.execute(b2)
        value = cursor.fetchone()
        # print(value)
        if value is not None:
            value = float(value[0])
        else:
            value = 0
        # print(value)
        BIPS_daily_data.append(value)
    return BIPS_daily_data
    cursor.close()
    s1.closeConn()


def insertArray(arrayBPI):
    # format list to tuple
    insert_values = tuple(arrayBPI)
    # print(arrayBPI)
    # print(insert_values)

    # start mysql connection
    s1 = MySQLConnector()
    s2 = s1.getConn()
    cursor = s2.cursor()

    # compile sql statement
    insert_statement = """insert into Basic_Performance_Info values('%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f)""" % insert_values

    insert_result = cursor.execute(insert_statement)
    s2.commit()
    if insert_result:
        print("[Success] Insert completed!")
    else:
        print("[Error] Insert failed!")
    cursor.close()
    s1.closeConn()

#
# pid = 'FB0009'
# bizdate = '2018-09-21'
# array = composeArray(pid, bizdate)
# print(array)
# insertArray(array)



