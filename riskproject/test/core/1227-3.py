import pandas as pd
import numpy as np
import sys

def get_value():
    df1 = pd.DataFrame({"A":["a","a","a","b","b","c","c","c","c"],"B":["a1","a2","a1","b2","b3","c4","c4","c1","c1"],"C":[1,2,3,1.5,2,1,1,1,4]})
    df2 = pd.DataFrame({"A":[11,222,333,444],"B":[555,6,6677,8],"C":[18,1,5671,1]})
    df3 = df1.append(df2)
    print(df1)

    xx = pd.pivot_table(df1,index=["A","B"], values=["C"], aggfunc=[len,np.sum])
    xx[u'总计'] = xx.sum(axis=1)
    print(xx)
    result_sum = pd.DataFrame(xx.sum()).T
    print(result_sum)
    return df1,df2


x = get_value()[1]

