# _*_ coding:utf-8 _*_
import time
import pandas as pd

t0 = time.time()

df = pd.read_csv(r'g:\20190124\szreportdata\szreport0.csv', index_col=0)

t1 = time.time()
print(t1-t0)

