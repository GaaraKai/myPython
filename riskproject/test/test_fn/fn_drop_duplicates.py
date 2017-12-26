import pandas as pd

df_trx_list = pd.DataFrame({"A":[1,2,3,4],"B":[5,6,9,8],"C":[1,1,1,1]})
print(df_trx_list)
#    A  B  C
# 0  1  5  1
# 1  2  6  1
# 2  3  6  1
# 3  4  8  1

df_trx_list1= df_trx_list.drop_duplicates(subset=['B','C'],keep='first')
print(df_trx_list1)