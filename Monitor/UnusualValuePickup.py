# -*- coding=utf-8 -*-

import configparser
from Common.GlobalConfig import GlobalConfig
from SQLStatement.BPI_html import BPIH
from SQLStatement.API_html import APIH
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
import pandas as pd


class UVP(object):
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

    def BPI_data_lookup(self):
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


    def API_data_lookup(self):
        # get API data
        apih = APIH()
        sqls = apih.setAPIH()
        ms = MySQLConnector()
        connection = ms.getConn()
        df = pd.read_sql(sql=sqls, con=connection)
        ms.closeConn()


