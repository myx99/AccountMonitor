import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import datasets, linear_model
from sklearn.cross_validation import train_test_split
from sklearn.linear_model import LinearRegression

from WindPy import w
from datetime import *
w.start()



codelist = ["000063.SZ", "002916.SZ", "002463.SZ", "603228.SH", "002436.SZ",
            "300476.SZ", "002446.SZ", "601231.SH", "603501.SH", "300602.SZ",
            "600308.SH", "002281.SZ", "300394.SZ"]
df_price = pd.DataFrame()
df_index = pd.DataFrame()

for cl in codelist:
    market_data = w.wsd(cl, "close", "2018-01-01", "2019-11-20", "Fill=Previous;Currency=CNY;PriceAdj=F")
    # print(market_data)
    df_price[cl] = market_data.Data[0]

# print(df_price)

market_index = w.wsd("399005.SZ", "close", "2018-01-01", "2019-11-20", "Fill=Previous;Currency=CNY;PriceAdj=F")
df_index["SME"] = market_index.Data[0]
# print(df_index)

X_train, X_test, y_train, y_test = train_test_split(df_price, df_index, random_state=1)
# print(X_train.shape)
# print(y_train.shape)
# print(X_test.shape)
# print(y_test.shape)


linreg = LinearRegression()
linreg.fit(X_train, y_train)

print(linreg.intercept_)
print(linreg.coef_)