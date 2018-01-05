import pandas as pd
import sys

def get_value():
    df1 = pd.DataFrame({"A":[1,2,3,4],"B":[5,6,7,8],"C":[1,1,1,1]})
    df2 = pd.DataFrame({"A":[11,222,333,444],"B":[555,6,6677,8],"C":[18,1,5671,1]})
    print('df1\n', df1)
    print('df1\n',df1.iloc[0:1])
    # sys.exit(0)
    # print('df2\n', df2.iloc[:,0].size)
    df3 = df1.append(df2)
    print(df3)
    print('df3\n', df3.iloc[:, 0].size)

    return df1,df2


x = get_value()[1]
print('x\n',x)
