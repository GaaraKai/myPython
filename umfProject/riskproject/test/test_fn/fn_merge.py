import pandas as pd

df1 = pd.DataFrame({'key': ['a', 'b', 'b'], 'date': ['1', '2', '3']})

df2 = pd.DataFrame({'key': ['a', 'b', 'c'], 'date': ['1', '5', '6']})

print('df1\n',df1)
print('df2\n',df2)

x = pd.merge(df1,df2,how='right', on=['date'])
x.columns = ['A','B','dB']
print(x)