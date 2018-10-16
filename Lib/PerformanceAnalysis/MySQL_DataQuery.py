import pandas as pd

from Lib import Common as ms


def db_query(fund_id, startdate, enddate, scale):
    # init db connection
    m = ms.MySQLConnection()
    conn = m.getConn()
    # conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='Matao_2012', db='test', use_unicode=True, charset="utf8")
    cur = conn.cursor()

    # compile sql statement
    if scale == "nav":
        select_statement = "select Occur_Date as 'Date', Subject_Name as 'NAV' from Valuation " \
                       "where Product_ID = '%s' " \
                       "and Occur_Date >= '%s' and Occur_Date <= '%s' " \
                       "and Subject_Code = '累计单位净值:' " \
                       "order by Occur_Date" % (fund_id, startdate, enddate)
    elif scale == "nav_increase":
        select_statement = "select Occur_Date as 'Date', Subject_Name as 'NAV_Increase' from Valuation " \
                           "where Product_ID = '%s' " \
                           "and Occur_Date >= '%s' and Occur_Date <= '%s' " \
                           "and Subject_Code = '净值日增长率(%s):' " \
                           "order by Occur_Date" % (fund_id, startdate, enddate, '%')
    else:
        print("please give a scale")
        return
    # print(select_statement)  # DEBUG

    # use pandas to get the sql result
    # df = pd.read_sql(sql=select_statement, con=conn, index_col="Date")
    df = pd.read_sql(sql=select_statement, con=conn)
    # print(df)  # DEBUG

    return df

    # close db connection
    cur.close()
    # conn.close()
    m.closeConn()

# test main
df = db_query('GZFB0001', '2017-01-18', '2017-02-28')
# print(df)
