# -*- coding:utf-8 -*-
# coding = utf-8
import csv
import pandas as pd
import pymysql
import sqlalchemy as sq
import time

# csvcontent = csv.reader(open('csv/171208180246__P1311000.csv', encoding='utf-8'))
csvcontent = csv.reader(open('csv/171208180246__P1311000.csv',encoding='GBK'))
# csvcontent = open('csv/171208180246__P1311000.csv')
# csv_line_cnt = len(open('csv/171208180246__P1311000.csv', "rU").readlines())
# print(csv_line_cnt)
# print(len(csvcontent.readlines()))
# print(csvcontent)
# print(len(csvcontent))
df_allTrx = pd.DataFrame({})
df_tdCapTrx = pd.DataFrame({})
df_zyCapTrx = pd.DataFrame({})
for line in csvcontent:
    if 1 < csvcontent.line_num <= 1000:
    # if 1 < csvcontent.line_num <= csv_line_cnt:
        # print(csvcontent.line_num)
        # print(len(line))
        allTrx = pd.DataFrame({'date': [line[6]], 'inst_trace': [line[1]], 'prod_id': [line[38][0:8]]})
        df_allTrx = df_allTrx.append(allTrx)
        if line[16] != "" and line[16] != '111111':
            tdCapTrx = pd.DataFrame({'date': [line[6]], 'prod_id': [line[38][0:8]], 'tdCapTrx': [line[16]]})
            df_tdCapTrx = df_tdCapTrx.append(tdCapTrx)
        if line[17] != "" and line[17] != '000000':
            zyCapTrx = pd.DataFrame({'date': [line[6]], 'prod_id': [line[38][0:8]], 'zyCapTrx': [line[17]]})
            df_zyCapTrx = df_zyCapTrx.append(zyCapTrx)
# print('df_allTrx\n',df_allTrx)
# print('df_tdCapTrx\n',df_tdCapTrx)
# print('df_zyCapTrx\n',df_zyCapTrx)
df_allTrx_grouped = df_allTrx.groupby([df_allTrx['date'], df_allTrx['prod_id']]).size().reset_index()
df_tdCapTrx_grouped = df_tdCapTrx.groupby([df_tdCapTrx['date'], df_tdCapTrx['prod_id']]).size().reset_index()
df_zyCapTrx_grouped = df_zyCapTrx.groupby([df_zyCapTrx['date'], df_zyCapTrx['prod_id']]).size().reset_index()
# print('df_allTrx_grouped\n',df_allTrx_grouped)
# print('df_tdCapTrx_grouped\n',df_tdCapTrx_grouped)
# print('df_zyCapTrx_grouped\n',df_zyCapTrx_grouped)
merge_td = pd.merge(df_allTrx_grouped, df_tdCapTrx_grouped, how='left', on=['date', 'prod_id'])
merge_td.columns = ['date', 'prod_id', 'trxCnt', 'tdCapCnt']
# print(merge_td)
merge_td_zy = pd.merge(merge_td, df_zyCapTrx_grouped, how='left', on=['date', 'prod_id'])
merge_td_zy.columns = ['date', 'prod_id', 'trxCnt', 'tdCapCnt', 'zyCapCnt']
# print(merge_td_zy)
merge_td_zy['tdCapRate'] = merge_td_zy['tdCapCnt'] / merge_td_zy['trxCnt']
merge_td_zy['zyCapRate'] = merge_td_zy['zyCapCnt'] / merge_td_zy['trxCnt']
merge_td_zy['batchNo'] = time.strftime("%Y%m%d%H%M%S")
print(merge_td_zy)

# # insert to DB
# engine = sq.create_engine('mysql+pymysql://root:root@127.0.0.1:3306/rcdb', echo=True)
# merge_td_zy.to_sql(name='ds_caprate', con=engine, if_exists='append', index=False, index_label=False)
