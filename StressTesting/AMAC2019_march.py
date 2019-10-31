from StressTesting.StockFilter import StockFilter
import pandas as pd
import numpy as np
from Common.GlobalConfig import GlobalConfig
from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
from StressTesting.BondFilter import BondFilter
from StressTesting.Bond_CategorySum import Bond_CategorySum
pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000)
np.set_printoptions(suppress=True)
from StressTesting.FutureFilter import FutureFilter


gc = GlobalConfig()
product_list_tmp = gc.getConfig('product', 'list')
product_list = product_list_tmp.split(", ")
# product_list.append('700001')
# print(product_list)
# product = "FB0003"
date = "20190228"

# print(index_list)
# print(stock_df_list)

df_beta = pd.read_excel("E:\\风控合规\\20190319_2019上半年压力测试\\压力测试-2019年上半年(已填写基础数据)\\beta.xlsx", sheetname="Sheet1")
# print(df_beta)
def stockbeta(product, date):
    sf = StockFilter(product, date)
    e, index_list, stock_df_list = sf.getdf()
    if index_list is None or stock_df_list is None:
        return "no data"
    for board, stock_df in zip(index_list, stock_df_list):
        if board == "SZ_HK_connect_prefix":
            print("HK stock doesn't fit in this formular! Skipping ......")
            continue
        # print(board)
        stock_df.loc[stock_df['L_SCLB'] == 1, 'VC_SCDM'] += '.SH'
        stock_df.loc[stock_df['L_SCLB'] == 2, 'VC_SCDM'] += '.SZ'
        beta_list = []
        for index, row in stock_df.iterrows():
            beta_temp = df_beta.loc[df_beta['Code'] == row['VC_SCDM'], 'Beta']
            if beta_temp.empty:
                beta = 1
            else:
                beta = beta_temp.values[-1]
            # print(beta)
            # print(beta[0])
            beta_list.append(beta)
        stock_df['beta'] = beta_list
        print(stock_df)
        beta_by_index = np.average(stock_df['beta'], weights=stock_df['EN_SZ'])
        value_by_index = stock_df['EN_SZ'].sum()
        print("Sub Market is :%s | stock sum value is :%.4f | average beta is : %.4f" % (board, value_by_index, beta_by_index))

# for p in product_list:
#     print("------ Product is : %s ------" % p)
#     stockbeta(p, date)

# stockbeta('FB0003', date)

def bond(product, date):
    bf = BondFilter(product, date)
    sqls = bf.get_allBond_sqls()
    ms = MySQLConnector()
    mysql_connection = ms.getConn()
    df = pd.read_sql(sql=sqls, con=mysql_connection)

    bcs = Bond_CategorySum()
    allbond = bcs.CreditCategory_AllBond(df)
    print(allbond)

def future(product, date):
    ff = FutureFilter(product, date)
    futuresqls = ff.get_sqls()
    ms = MySQLConnector()
    ms_connection = ms.getConn()
    df = pd.read_sql(sql=futuresqls, con=ms_connection)

    bondfuture_sum = 0
    if_sum = 0
    ic_sum = 0
    ih_sum = 0
    for index, row in df.iterrows():
        future_code = row['VC_SCDM']
        if future_code[0] == 'T':
            bondfuture_sum += row['EN_SZ']
        if future_code[0:1] == 'IF':
            if_sum += row['EN_SZ']
        if future_code[0:1] == 'IC':
            ic_sum += row['EN_SZ']
        if future_code[0:1] == 'IH':
            ih_sum += row['EN_SZ']

    bondfuture_mild = bondfuture_sum * -0.0151
    bondfuture_moderate = bondfuture_sum * -0.0215
    bondfuture_severe = bondfuture_sum * -0.0272
    print("Bond Future result: %.4f, %.4f, %.4f" % (bondfuture_mild, bondfuture_moderate, bondfuture_severe))

    if_case = indexfuture('IF')
    ih_case = indexfuture('IH')
    ic_case = indexfuture('IC')

    indexfuture_if_mild = if_sum * if_case[0]
    indexfuture_if_moderate = if_sum * if_case[1]
    indexfuture_if_severe = if_sum * if_case[2]

    indexfuture_ih_mild = ih_sum * ih_case[0]
    indexfuture_ih_moderate = ih_sum * ih_case[1]
    indexfuture_ih_severe = ih_sum * ih_case[2]

    indexfuture_ic_mild = ic_sum * ic_case[0]
    indexfuture_ic_moderate = ic_sum * ic_case[1]
    indexfuture_ic_severe = ic_sum * ic_case[2]

    loss_mild = bondfuture_mild + indexfuture_if_mild + indexfuture_ih_mild + indexfuture_ic_mild
    loss_moderate = bondfuture_moderate + indexfuture_if_moderate + indexfuture_ih_moderate + indexfuture_ic_moderate
    loss_severe = bondfuture_severe + indexfuture_if_severe + indexfuture_ih_severe + indexfuture_ic_severe

    print("轻： %.4f | 中： %.4f | 重： %.4f" % (loss_mild, loss_moderate, loss_severe))


def indexfuture(type):
    df_index = pd.read_excel("E:\\风控合规\\20190319_2019上半年压力测试\\indexfuture.xlsx", sheetname=type)
    beta_list = []
    board_list = []
    loss_mild_list = []
    loss_moderate_list = []
    loss_severe_list = []
    for index, row in df_index.iterrows():
        stockid = row['Code']
        weight = row['Weight'] / 100
        beta_temp = df_beta.loc[df_beta['Code'] == stockid, 'Beta']
        if beta_temp.empty:
            beta = 1
        else:
            beta = beta_temp.values[-1]
        beta_list.append(beta)

        sqls = """select s_info_listboard from wind.AShareDescription where S_INFO_WINDCODE = '%s'""" % stockid
        oc = OracleConnector()
        oracle_connection = oc.getConn()
        df_board = pd.read_sql(sql=sqls, con=oracle_connection)
        board = df_board.iat[0, 0]
        board_list.append(board)

        if board == '434004000':
            loss_mild = beta * weight * -0.1499
            loss_moderate = beta * weight * -0.218
            loss_severe = beta * weight * -0.269
        elif board == '434003000':
            loss_mild = beta * weight * -0.1697
            loss_moderate = beta * weight * -0.2453
            loss_severe = beta * weight * -0.3013
        elif board == '434001000':
            loss_mild = beta * weight * -0.1693
            loss_moderate = beta * weight * -0.2441
            loss_severe = beta * weight * -0.3001
        loss_mild_list.append(loss_mild)
        loss_moderate_list.append(loss_moderate)
        loss_severe_list.append(loss_severe)

    df_index['beta'] = beta_list
    df_index['board'] = board_list
    df_index['loss_mild'] = loss_mild_list
    df_index['loss_moderate'] = loss_moderate_list
    df_index['loss_severe'] = loss_severe_list

    sum_loss_mild = df_index['loss_mild'].sum()
    sum_loss_moderate = df_index['loss_moderate'].sum()
    sum_loss_severe = df_index['loss_severe'].sum()
    #
    # loss_by_board_mild = 0
    # loss_by_board_moderate = 0
    # loss_by_board_severe = 0
    # for b in board_list:
    #     df_index_byboard = df_index.loc[df_index['board'] == b]
    #     # df_index_byboard['newweight'] = df_index_byboard['Weight'] / df_index_byboard['Weight'].sum()
    #     beta_by_index = np.average(df_index_byboard['beta'], weights=df_index_byboard['Weight'])
    #     value_by_index = df_index_byboard['Weight'].sum() / 100
    #     # print(beta_by_index, value_by_index)
    #     if b == '434004000':
    #         loss_mild = beta_by_index * value_by_index * -0.1499
    #         loss_moderate = beta_by_index * value_by_index * -0.218
    #         loss_severe = beta_by_index * value_by_index * -0.269
    #     elif b == '434003000':
    #         loss_mild = beta_by_index * value_by_index * -0.1697
    #         loss_moderate = beta_by_index * value_by_index * -0.2453
    #         loss_severe = beta_by_index * value_by_index * -0.3013
    #     elif b == '434001000':
    #         loss_mild = beta_by_index * value_by_index * -0.1693
    #         loss_moderate = beta_by_index * value_by_index * -0.2441
    #         loss_severe = beta_by_index * value_by_index * -0.3001
    #     loss_by_board_mild += loss_mild
    #     loss_by_board_moderate += loss_moderate
    #     loss_by_board_severe += loss_severe
    # if type == 'IF':
    #     mild_pctg = loss_by_board_mild / 3669.37
    #     moderate_pctg = loss_by_board_moderate / 3669.37
    #     severe_pctg = loss_by_board_severe / 3669.37
    # elif type == 'IC':
    #     mild_pctg = loss_by_board_mild / 5025.29
    #     moderate_pctg = loss_by_board_moderate / 5025.29
    #     severe_pctg = loss_by_board_severe / 5025.29
    # elif type == 'IH':
    #     mild_pctg = loss_by_board_mild / 2743.97
    #     moderate_pctg = loss_by_board_moderate / 2743.97
    #     severe_pctg = loss_by_board_severe / 2743.97

    # print("%s  -  轻： %.4f | 中： %.4f | 重： %.4f" % (type, sum_loss_mild, sum_loss_moderate, sum_loss_severe))

    return [sum_loss_mild, sum_loss_moderate, sum_loss_severe]

for p in product_list:
    print("------ Product: %s ------" % p)
    future(p, date)

# future("FB0001", date)
#
# indexfuture('IF')
# indexfuture('IC')
# indexfuture('IH')
