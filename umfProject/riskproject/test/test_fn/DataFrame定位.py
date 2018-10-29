import numpy as np
import pandas as pd
df = pd.DataFrame(np.arange(0,60,2).reshape(10,3),columns=list('abc'))
print(df)
print(df.loc[2,'a'] )
print(df.loc[[1, 5], ['b', 'c']] )
print(df.loc[0:, ['b', 'c']] )

df1 = pd.Series(('11','12412'),index=['qw','er'])
print(df1.get('qw'))