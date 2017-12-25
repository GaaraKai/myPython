import pandas as pd
import numpy as np
df = pd.DataFrame({'AAA' : [4,5,6,7], 'BBB' : [10,20,30,40],'CCC' : [100,50,-30,-50]})
print(df)
df.loc[df.AAA >= 5,'BBB'] = -1 #when AAA >= 5 set bbb = -1
print(df)
df.loc[df.AAA >= 5,['BBB','CCC']] = 555 #when AAA >= 5 set bbb = -555 and ccc= -555
print(df)
df_mask = pd.DataFrame({'AAA' : [True] * 4, 'BBB' : [False] * 4,'CCC' : [True,False] * 2})
print(df_mask)
df.where(df_mask,-1000)
print(df)
df =[]
df = pd.DataFrame({'AAA' : ['',5,6,7], 'BBB' : [10,20,30,40],'CCC' : [100,50,-30,-50]})
print(df)
df['logic'] = np.where(df['AAA'] =='','high','low')
print(df)
df = pd.DataFrame({'AAA' : [4,5,6,7], 'BBB' : [10,20,30,40],'CCC' : [100,50,-30,-50]})
print(df)
aValue = 43
# print(df.CCC)
print(df.loc[(df.CCC-aValue).abs().argsort()])
df = pd.DataFrame({'AAA' : [4,5,6,7], 'BBB' : [10,20,30,40],'CCC' : [100,50,-30,-50]})
print(df)
print(df[(df.AAA <= 6) & (df.index.isin([0,2,4]))])