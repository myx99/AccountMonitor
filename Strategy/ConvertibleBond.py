from WindPy import *
import pandas as pd

fund = 10000000

class trade(object):
    def __init__(self):
        self.balance = fund
        self.position = 0
        self.commissionrate = 0.001
        self.df_traderecord = pd.DataFrame(columns={"time", "side", "quantity", "price"})
        self.buy_lot = 600
        self.sell_lot = 400
        self.lot_delta = 100
        self.orderbasket = []
        w.start()
        # print(self.df_traderecord)

    def buy(self, time, quantity, price):
        # validation
        commissioncost = quantity * price * self.commissionrate
        transaction_amount = quantity * price + commissioncost
        if transaction_amount > self.balance:
            print("[Error] Transaction failed: Unsufficient Usable Cash")
            return 0
        else:
            commissioncost = quantity * price * self.commissionrate
            self.balance -= (quantity * price + commissioncost)
            self.position += quantity
            df = pd.DataFrame({"time": [time], "side": ["buy"], "quantity": [quantity], "price": [price]})
            # print(df)
            self.df_traderecord = self.df_traderecord.append(df,ignore_index=True)
            # print(self.df_traderecord)
            # self.df_traderecord = pd.merge(self.df_traderecord, df, how='outer', on='time')
            print("[Trade Log] Transaction finished: %s     buy     %d    %.4f" % (time, quantity, price))

    def sell(self, time, quantity, price):
        # validation
        if quantity > self.position:
            print("[Error] Transaction failed: Unsufficient Usable Share")
            return 0
        else:
            commissioncost = quantity * price * self.commissionrate
            self.balance += (quantity * price + commissioncost)
            self.position -= quantity
            df = pd.DataFrame({"time": [time], "side": ["sell"], "quantity": [quantity], "price": [price]})
            # print(df)
            self.df_traderecord = self.df_traderecord.append(df, ignore_index=True)
            # print(self.df_traderecord)
            # self.df_traderecord = pd.merge(self.df_traderecord, df, how='outer', on='time')
            print("[Trade Log] Transaction finished: %s     sell     %d    %.4f" % (time, quantity, price))

    def checkstatus(self):
        pass

    def cancelbuy(self, time, quantity, price):
        pass


    def marketdata(self):
        data = w.wsd("113015.SH", "close,convpremiumratio,strbpremiumratio", "2018-01-01", "2018-12-31", "Currency=CNY")
        df_mktdata = pd.DataFrame()
        df_mktdata["date"] = data.Times
        df_mktdata["closeprice"] = data.Data[0]
        df_mktdata["convpremiumratio"] = data.Data[1]
        df_mktdata["strbpremiumratio"] = data.Data[2]
        print(df_mktdata)
        return df_mktdata

    def strats_1(self, df):
        # 当纯债溢价率strbpremiumratio超过20%买入，当转股溢价率convpremiumratio超过40%卖出
        for index, row in df.iterrows():
            date = row["date"]
            price = row["closeprice"]
            convpremiumratio = row["convpremiumratio"]
            strbpremiumratio = row["strbpremiumratio"]
            buy_lot = self.buy_lot
            sell_lot = self.sell_lot

            if strbpremiumratio > 0.2:
                if self.buy(date, buy_lot, price) == 0:
                    while self.buy(date, buy_lot, price) == 0:
                        buy_lot = buy_lot - self.lot_delta
                        if buy_lot <= 0:
                            break

            if convpremiumratio > 0.4:
                if self.sell(date, sell_lot, price) == 0:
                    while self.sell(date, sell_lot, price) == 0:
                        sell_lot = sell_lot - self.lot_delta
                        if sell_lot <= 0:
                            break

        print(self.df_traderecord)
        print("final balance: %.2f" % self.balance)
        print("final position: %d" % self.position)
        pnl = self.balance + self.position * df["closeprice"].values[-1] - fund
        rate = ( pnl / fund ) * 100
        print(pnl)
        print("PnL : %.2f" % pnl)
        print("Return Rate : %.4f %%" % rate)


if __name__ == '__main__':
    t = trade()
    df = t.marketdata()
    t.strats_1(df)
