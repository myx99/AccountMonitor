# -*- coding=utf-8 -*-

import configparser
from Common.GlobalConfig import GlobalConfig
from SQLStatement.BPI_html import BPIH
from SQLStatement.API_html import APIH
from SQLStatement.BPI_select import BPIS
from SQLStatement.API_select import APIS
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
import pandas as pd


class BPI_data_lookup(object):
    def __init__(self):
        # get threshold
        config = GlobalConfig()
        path = config.getConfig('SubConfigPath', 'monitor_win')
        # path = config.getConfig('SubConfigPath', 'monitor_linux')
        # print(path)
        self.cp = configparser.ConfigParser()
        self.cp.read(path, encoding='utf-8-sig')

    def getConfig(self, section, key):
        return self.cp.get(section, key)

    def NAV_monitor(self):
        # get BPI data
        bpih = BPIH()
        sqls = bpih.setBPIH()
        ms = MySQLConnector()
        connection = ms.getConn()
        df = pd.read_sql(sql=sqls, con=connection)
        ms.closeConn()

        collector = []
        # nav data
        nav_monitor_list = self.getConfig('NAV_threshold', 'nav_monitor_list')
        nav_monitor_list = nav_monitor_list.split(", ")
        for n in nav_monitor_list:
            nav_threshold = self.getConfig('NAV_threshold', n)
            nav_threshold = nav_threshold.split(", ")
            nav_warning = nav_threshold[1]
            nav_stoploss = nav_threshold[0]
            distance_to_warning = df.loc[df['Product_ID'] == n, 'Unit_NAV'] - nav_warning
            distance_to_stoploss = df.loc[df['Product_ID'] == n, 'Unit_NAV'] - nav_stoploss
            nav_monitor_result = '[Normal] Unit NAV monitoring of %s is ok' % n
            if distance_to_warning <= 0:
                nav_monitor_result = '[Warning]Unit NAV monitoring of %s is Touching Warning Line' % n
            if distance_to_stoploss <= 0:
                nav_monitor_result = '[Warning]Unit NAV monitoring of %s is Touching Stop-Loss Line' % n
            collector.append(nav_monitor_result)

        # different assets percentage

    #
    # def API_data_lookup(self):
    #     # get API data
    #     apih = APIH()
    #     sqls = apih.setAPIH()
    #     ms = MySQLConnector()
    #     connection = ms.getConn()
    #     df = pd.read_sql(sql=sqls, con=connection)
    #     ms.closeConn()

class API_data_lookup(object):
    def __init__(self, productID, EndDate=None):
        # initiate mysql connection
        ms = MySQLConnector()
        connection = ms.getConn()

        # check duration start date
        bpis = BPIS()
        valuation_table = bpis.getConfig('Valuation_Table_Info', 'table')
        sqls_startdate = """ select left(D_YWRQ,10) as BizDate from %s
                              where VC_CPDM = '%s' and VC_KMDM like '基金单位净值%%' and EN_SZ = 1 order by left(D_YWRQ,10) desc limit 1""" \
                        % (valuation_table, productID)
        df_startdate = pd.read_sql(sql=sqls_startdate, con=connection)
        StartDate = df_startdate["BizDate"].values[-1]

        # trade day and natural day check
        StartDate_formatted = StartDate.replace("-", "")
        print("Static based on the start data of : %s" % StartDate_formatted)
        td = TradingDay()
        self.naturaldays = td.getNaturalDayCounts(StartDate_formatted, EndDate)
        self.tradedays = td.getTradingDayCounts(StartDate_formatted, EndDate)
        print(self.naturaldays)
        print(self.tradedays)

        # process for data
        m = APIS()
        sqls = m.setAPIS_withStartdate(productID, StartDate, EndDate)
        self.df = pd.read_sql(sql=sqls, con=connection)
        # print(self.df)

        # close mysql connection
        ms.closeConn()

        # define in class var
        self.productid = productID
        self.startdate = StartDate
        self.enddate = EndDate

    def duration_annualized_return(self):
        # add check logic for trade/natural day calculation
        # naturalday = pd.period_range(self.df['Occur_Date'].iloc[0], self.df['Occur_Date'].iloc[-1], freq='D')
        # tradeday = len(self.df['Occur_Date'])

        # print("Numbers of natural day: %d | checker: %d" % (naturalday.__len__(), self.checker_naturaldays))
        # print("Numbers of trading day: %d | checker: %d" % (tradeday, self.checker_tradedays))
        navdelta = self.df['Unit_NAV'].iloc[-1].astype(float) - 1
        # an = navdelta * 365 / naturalday.__len__()
        # an = navdelta * 365 / self.naturaldays
        an = navdelta * 365 / (self.naturaldays - 1)
        print("Annualized Return: %.4f" % an)
        # ar_list = [naturalday.__len__(), tradeday, round(an, 4)]
        return an

    def diagnosis(self, an):
        bpidl = BPI_data_lookup()
        targetAnnualizedReturn = bpidl.getConfig("Duration_AnnualizedReturn", self.productid)
        targetAnnualizedReturn = float(targetAnnualizedReturn)
        print("Target Annualized Return: %.4f" % targetAnnualizedReturn)
        angap = an - targetAnnualizedReturn
        alert_level_10percent = targetAnnualizedReturn * 0.1 * -1
        alert_level_5percent = targetAnnualizedReturn * 0.05 * -1
        alert_level_2percent = targetAnnualizedReturn * 0.02 * -1
        if angap <= alert_level_10percent:
            status = "Level-1 Alert"
        elif angap <= alert_level_5percent and angap > alert_level_10percent:
            status = "Level-2 Alert"
        elif angap <= alert_level_2percent and angap > alert_level_5percent:
            status = "Level-3 Alert"
        else:
            status = "Normal"
        return [targetAnnualizedReturn, angap, status, self.startdate]

    def AN_monitor_Insert(self, insert_data):
        # start mysql connection
        ms = MySQLConnector()
        msc = ms.getConn()
        cursor = msc.cursor()
        # compile sql statement
        insert_statement = """insert into Annualized_Return_Monitor values('%s','%s','%s',%f,%f,%f,'%s')""" % insert_data
        insert_result = cursor.execute(insert_statement)
        msc.commit()
        if insert_result:
            print("[Success] Insert completed!")
        else:
            print("[Error] Insert failed!")
        cursor.close()
        ms.closeConn()



if __name__ == '__main__':
    # p = 'FB0006'
    enddate = '20191118'
    #
    # print("========================================")
    # print("Product ID : %s" % p)
    # print("Static based on the end data of : %s" % enddate)
    #
    # uvpa = API_data_lookup(p, enddate)
    # an = uvpa.duration_annualized_return()
    # result = uvpa.diagnosis(an)
    # angap = result[0]
    # status = result[-1]
    #
    # print("Annualized - Target = %.4f" % angap)
    # print("Alert Level: %s" % status)
    # print("========================================")

    bpidl = BPI_data_lookup()
    product_list_tmp = bpidl.getConfig("Duration_AnnualizedReturn", "product_list")
    product_list = product_list_tmp.split(", ")
    print(product_list)

    for p in product_list:
        print("========================================")
        print("Product ID : %s" % p)
        print("Static based on the end data of : %s" % enddate)
        uvpa = API_data_lookup(p, enddate)
        an = uvpa.duration_annualized_return()
        result = uvpa.diagnosis(an)
        targetAnnualizedReturn = result[0]
        angap = result[1]
        status = result[2]
        startdate = result[3]
        print("Annualized - Target = %.4f" % angap)
        print("Alert Level: %s" % status)
        print("========================================")
        # Compose Array
        # insert_data = (enddate, p, startdate, an, targetAnnualizedReturn, angap, status)
        # print(insert_data)
        #
        # uvpa.AN_monitor_Insert(insert_data)
