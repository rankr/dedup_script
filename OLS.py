#coding: utf-8
import numpy as np
import pandas as pd
import statsmodels.api as sm

p = '/Users/file4/毕设/data/bitcoin_delta_depth_rate.csv'
data = pd.read_csv(p)
dataset = np.array(data)

X1 = np.array([[1, x] for x in dataset[:-3, 1]])
Y1 = dataset[:-3, 0]

res = sm.OLS(Y1, X1).fit()

print res.summary()