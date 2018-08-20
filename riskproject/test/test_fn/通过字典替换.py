import pandas as pd
import numpy as np
import sys

d= {"1":"电商商户"
,"2":"平台"
,"3":"基金商户"
,"4":"彩票商户"
,"5":"航旅商户"
,"6":"P2P商户"
,"7":"报销商户"
,"8":"数字娱乐商户"
,"9":"电渠"
,"10":"行业资金归集"
,"11":"个人金融业务线"
,"12":"其它"
,"13":"移动电商"
,"14":"互联网理财"
,"15":"消费金融"
,"16":"代理商"
,"17":"客服"
,"18":"跨境"
,"19":"聚合服务商"
,"20":"电商三类"
,"c4":"银行(自用)2"
    , "a": "银行(自用)"}
print(d)

df1 = pd.DataFrame({"A": ["a", "a", "a", "b", "b", "c", "c", "c", "c"],
                    "B": ["a1", "a2", "a1", "b2", "b3", "c4", "c4", "c1", "c1"],
                    "C": [1.5, 2, 3, 1.5, 2, 1, 1, 1.5, 4]})
print(df1)
df1.replace(d, inplace=True)
print(df1)

tel = ['2345', '8012', '1257']
name = ['Lily', 'Tom', 'Jack']
print('Find Lily', tel[name.index('Lily')])

# dict
phonebook = {'Lily': '2345', 'Tom': '8012', 'Jack': '1257'}  # attention {}
print('Find Lily', phonebook['Tom']  )
