import pandas as pd
import time
import os
import sys
import numpy as np

def get_prod_id(row):
    a = row['支付产品编号']
    if pd.isnull(a):
        pass
    else:
        return a[0:8]

def get_trx_detail(reader):
    pass


# 2. Get csv dict by csv file path
reader = pd.read_csv("C://Users//Administrator//PycharmProjects//myPython//docs/csvfiles//ds_stat//180102101644__P1350000.csv",
                     encoding='gbk', chunksize=40, iterator=True, dtype=str)
rtn_df = pd.DataFrame({})
y = pd.DataFrame({})
prod_id = pd.Series()
for row in reader:
    x = row[['机构号','同盾设备指纹', '自研设备指纹', '平台日期']]
    rtn_df = rtn_df.append(x)
    prod_id_list = row.apply(get_prod_id, axis=1)
    prod_id = prod_id.append(prod_id_list)
    rtn_df["prod_id"] = prod_id
rtn_df.rename(columns={'机构号':'inst_id','同盾设备指纹': 'td_device'
        , '自研设备指纹': 'rtn_df_dropnay_device', '平台日期': 'plat_date'}, inplace=True)
print(rtn_df)
# print(len(rtn_df))
trx_cnt = pd.pivot_table(rtn_df,index=["plat_date","prod_id"],values="inst_id",aggfunc=[len])
print('trx_cnt\n',trx_cnt)
rtn_df_dropna = rtn_df.dropna(axis=0,how='any')
# print(len(rtn_df_dropna))
# print(rtn_df_dropna)


# age = rtn_df['td_device']
# age_null = pd.isnull(age)
# print('age_null\n',age_null)
# print('len age_null\n',len(age_null))
# age_null_true = age[age_null]
# print(age_null_true)
# age_null_cnt = len(age_null_true)
# print(age_null_cnt)
#
# xx = rtn_df['td_device'][age_null == False]
# print('xx \n',xx)

rtn_df_drop11 = rtn_df_dropna[rtn_df_dropna['td_device'] != "111111"]
# print('rtn_df_drop11\n',rtn_df_drop11)


td_cap_cnt = pd.pivot_table(rtn_df_drop11,values='td_device',index=['plat_date',"prod_id"],aggfunc=[len])
print('td_cap_cnt\n',td_cap_cnt)

print("-------------")
trx_cnt = trx_cnt.reset_index()
td_cap_cnt = td_cap_cnt.reset_index()
print('new trx_cnt\n',trx_cnt)
print('new td_cap_cnt\n',td_cap_cnt)
print("-------------")
merge_td = pd.merge(trx_cnt, td_cap_cnt, how='left', on=['plat_date', 'prod_id'])
print("merge_td\n",merge_td)
merge_td.rename(columns={'len_x': 'trx_cnt','len_y': 'td_cap_cnt'}, inplace=True)
print("new merge_td\n",merge_td)

# 3. Process Csv Dict to Get All Transactions as df_trx_detail



df_trx_detail = get_trx_detail(reader)