import configparser
from Common.GlobalConfig import GlobalConfig
from Common.TradingDay import TradingDay
from Common.MySQLConnector import MySQLConnector
from Common.OracleConnector import OracleConnector
import pandas as pd


class Bond(object):
    def __init__(self):
        pass
    def getBondInfo(self, bondcode):
        oc = OracleConnector()


    def singleBondDuration(self, face, coupon, maturity, discount, interest_frequency, paymenttype):  # face 面值 coupon 票面 maturity 至行权年限 discount 最新估值
        if interest_frequency == 'Y1': # Y1-year, M6-half year, M3-season, M1-monnth
            frequency = 1
        elif interest_frequency == 'M6':
            frequency = 6/12
        elif interest_frequency == 'M3':
            frequency = 3/12
        elif interest_frequency == 'M1':
            frequency = 1/12

        if paymenttype == 'simple':
            pass #TODO:
        elif paymenttype == 'compound':
            pass #TODO:



        if maturity <= 1:
            price = (face + (coupon * face)) / (1 + discount) ** maturity
            dmac = maturity
            dmod = dmac / (1 + discount)
        else:
            discounted_final_cf = (face + (coupon * face)) / (1 + discount) ** maturity
            dmac = discounted_final_cf * maturity
            maturity -= frequency
            x = 0

            while maturity > 0:
                # discounted_cf = (coupon * face) + (discounted_cf / (1 + discount) ** maturity)
                discounted_cf = coupon * face / (1 + discount) ** maturity
                x += discounted_cf
                dmac = dmac + discounted_cf * maturity
                maturity -= 1

            price = x + discounted_final_cf
            dmac = dmac / price
            dmod = dmac / (1 + discount)

        # print("Price : %.4f" % price)
        # print("Duration : %.4f" % dmac)
        print("Amend Duration : %.4f" % dmod)
        return dmod


if __name__ == '__main__':
    face = 100
    # coupon = 0.07
    # maturity = 0.3647
    # discount = 0.050163
    frequency = 1/12
    coupon = 0.0705
    maturity = 0.0548
    discount = 0.063226
    b = Bond()
    b.singleBondDuration(face, coupon, maturity, discount)