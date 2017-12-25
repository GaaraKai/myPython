import numpy as np
import pandas as pd

np.random.seed(1)
# d1 = pd.Series(2*np.random.normal(size = 100)+3)
d1 = pd.Series(np.random.normal(size = 100))
# d2 = np.random.f(2,4,size = 100)
# d3 = np.random.randint(1,100,size = 100)
def status(x) :
    return pd.Series([x.count(),x.min(),x.idxmin(),x.quantile(.25),x.median(),
                      x.quantile(.75),x.mean(),x.max(),x.idxmax(),x.mad(),x.var(),
                      x.std(),x.skew(),x.kurt()],index=['总数','最小值','最小值位置','25%分位数',
                    '中位数','75%分位数','均值','最大值','最大值位数','平均绝对偏差','方差','标准差','偏度','峰度'])
df = pd.DataFrame(status(d1))
print(df)


x = pd.Series([135231,222,312,41523],index=['asdf','asdf','43dfg','dfgr'])
print(x)
