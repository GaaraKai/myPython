import pandas as pd
import numpy as np

s1=pd.Series(np.array([1,2,3,4]))
s2=pd.Series(np.array([5,6,7,8]))
df=pd.DataFrame({"a":s1,"b":s2})
print(type(s1))
print(df)

print('-----------------')
df1=pd.DataFrame({"A":[1,2,3,4],"B":[5,6,7,8],"C":[1,1,1,1]})
print(df1)
df1.ix[df1.A>1,'B']= -1
print(df1)
print('//////////////')
df2=pd.DataFrame({"A":[1,3,3,4],"B":[5,6,7,8],"C":[1,1,1,1]})
df2["then"]=np.where(df2.A<3,1,0)
print(df2)
print('33333333')
df3=pd.DataFrame({"A":[1,2,3,4],"B":[5,6,7,8],"C":[1,1,1,1]})
df3=df3[df3.A>=2]
print(df3)
print('44444444444')
df4 = pd.DataFrame({'animal': 'cat dog cat fish dog cat cat'.split(),
                  'size': list('SSMMMLL'),
                  'weight': [8, 10, 11, 1, 20, 12, 12],
                  'adult' : [False] * 5 + [True] * 2});
print(df4)
group=df4.groupby("animal")
print(group)
cat=group.get_group("cat")
print(cat)