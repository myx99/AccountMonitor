from StressTesting.StockFilter import StockFilter
from StressTesting.Stock_BetaCal import Stock_BetaCal
from StressTesting.Stock_StressScenarioCal import Stock_StressCalculate
from StressTesting.BondFilter import BondFilter
from StressTesting.Bond_CategorySum import Bond_CategorySum
from StressTesting.Bond_StressScenarioCal import Bond_StressCalculate
from Common.MySQLConnector import MySQLConnector
import pandas as pd
from Common.GlobalConfig import GlobalConfig
import configparser
from Common.TradingDay import TradingDay


class DGSSC(object):
    def __init__(self, product, enddate=None):
        self.product = product
        self.enddate = enddate
        td = TradingDay()
        ltd = td.getLastTradingDay()
        if enddate is None:
            self.enddate = ltd
        # self.df = pd.DataFrame(columns=['Occur_Date', 'Product_ID', 'Category', 'Mild', 'Moderate', 'Severe'])

    def Stock_Stress_Data(self):
        sf = StockFilter(self.product, self.enddate)
        e, x, y = sf.getdf()
        sbc = Stock_BetaCal()
        z = sbc.Beta(e, x, y)
        ssc = Stock_StressCalculate()
        stock_stress_result = ssc.StockLoss(z)
        # print(stock_stress_result)
        return stock_stress_result

    def Bond_Stress_Data(self):
        bf = BondFilter(self.product, self.enddate)
        sqls_cb = bf.get_CBond_sqls()
        sqls_pb = bf.get_PBond_sqls()
        ms = MySQLConnector()
        mysql_connection = ms.getConn()
        df_cb = pd.read_sql(sql=sqls_cb, con=mysql_connection)
        df_pb = pd.read_sql(sql=sqls_pb, con=mysql_connection)

        bcs = Bond_CategorySum()
        cb = bcs.Category_CB(df_cb)
        pb = bcs.CreditCategory_PB(df_pb)

        bsc = Bond_StressCalculate()
        pbb = bsc.PureBondBreak(pb)
        pbl = bsc.PureBondLoss(pb)
        cbl = bsc.CBondLoss(cb)

        result_list = []
        result_list.append(pbb)
        result_list.append(pbl)
        result_list.append(cbl)
        # print(pbb)
        # print(pbl)
        # print(cbl)
        return result_list

    def Data_Insert(self):
        stock_stress_result = self.Stock_Stress_Data()
        bond_stress_result_list = self.Bond_Stress_Data()
        # print(stock_stress_result)
        # print(bond_stress_result_list)

        if stock_stress_result is None and bond_stress_result_list is None:
            print("No Stress Testing Data....")
            return

        bond_stress_result_list.append(stock_stress_result)

        ms = MySQLConnector()
        msc = ms.getConn()
        cursor = msc.cursor()
        for bsr in bond_stress_result_list:
            if bsr is not None:
                DGSSC_daily_data = (self.enddate, self.product, bsr[0], bsr[1], bsr[2], bsr[3])
                print(DGSSC_daily_data)

                # compile sql statement
                insert_statement = """insert into Stress_Testing_Data values('%s','%s','%s',%f,%f,%f)""" % DGSSC_daily_data
                insert_result = cursor.execute(insert_statement)
                msc.commit()
                if insert_result:
                    print("[Success] Insert completed!")
                else:
                    print("[Error] Insert failed!")
        cursor.close()
        ms.closeConn()


if __name__ == '__main__':
    product = "FB0009"
    date = "20190211"
    dgssc = DGSSC(product, date)
    dgssc.Data_Insert()
