import pandas as pd
import sys

df1 = pd.DataFrame({"A":['111232','','3345345','4123123'],"B":[5123123,6,7,8],"C":[1242141,1,1,1]})
df1 = df1[df1["A"] != ""]
print(df1)


sys.exit(0)
# sys.exit(0)
a =[]
y = pd.Series([11,22,33,44])
# df1['A'] = df1['A'].astype('str')
# print(df1['A'])
print('---------')
for i in df1['A']:
    # print('i = ',i)
    # print('i[0:4] = ', i[0:4])
    a.append(i[0:4])

print(a)
df1["Z"] = df1["A"]
df1["Y"] = a
df1["X"] = df1["C"] + df1["B"]
print(df1)