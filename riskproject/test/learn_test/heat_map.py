import matplotlib.pyplot as plt
import numpy as np
# np.random.seed(0)
import seaborn as sns
import pandas as pd
import csv
import datetime
import sys
# % matplotlib inline

start_time = datetime.datetime.now()



flights = pd.read_csv('D:/github_program/myPython/riskproject/test/learn_test/heat_map.csv')
flights["tdCapRate"] = flights['tdCapRate'].astype('float64')
print(flights.columns)
flights = flights.pivot("payDate", "procID", "tdCapRate")
print(flights.where(flights.notnull(), 0))
flights = flights.where(flights.notnull(), 0)
flights = flights[-20:-1]
print (flights.tail())

f, ax = plt.subplots(figsize = (14, 10))

ax = sns.heatmap(flights,annot=True,linewidths=.5,yticklabels=False)
ax.set_title("Correlation between features")
ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
ax.set_yticklabels(ax.get_yticklabels(), rotation=45)



plt.show()

end_time = datetime.datetime.now()
print('start_time =', start_time)
print('end_time   =', end_time)
print('diff_Time =', end_time - start_time)
print('System Processing Done...')