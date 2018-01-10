import pandas as pd
import time
import os
import sys
import numpy as np

def get_trx_detail(reader):
    pass


# 2. Get csv dict by csv file path
reader = pd.read_csv("D:/github_program/myPython/docs//csvfiles/ds_stat//180102102033__P1311000.csv",
                     encoding='gbk', chunksize=40, iterator=True, dtype=str)
rtn_df = pd.DataFrame({})
for row in reader:
    x = row[['同盾设备指纹', '自研设备指纹', '平台日期']]
    rtn_df = rtn_df.append(x)
rtn_df.rename(columns={'同盾设备指纹': 'td_device'
        , '自研设备指纹': 'zy_device'
        , '平台日期': 'plat_date'}, inplace=True)
print(rtn_df)
print(len(rtn_df))
pd.pivot_table(rtn_df,index=[""],values=["Price"],aggfunc=np.sum)





# 3. Process Csv Dict to Get All Transactions as df_trx_detail



df_trx_detail = get_trx_detail(reader)