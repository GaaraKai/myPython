import pandas as pd
import time
import os
import sys

# ds_cap_rate


def get_ds_cap_rate(csv_reader):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name,
          sys._getframe().f_lineno, 'csv_reader = ', csv_reader)
    df_all_trx = pd.DataFrame({})
    df_td_cap_trx = pd.DataFrame({})
    df_zy_cap_trx = pd.DataFrame({})
    for line in csv_reader:
        # if 1 < csv_reader.line_num < 400:
            order_date = line['商户订单日期']
            inst_trace = line['机构请求流水']
            prod_id = line['支付产品编号'][0:8]
            if len(prod_id) == 8:
                # 1.Count All Transaction & Group By
                all_trx = pd.DataFrame({'order_date': [order_date], 'inst_trace': [inst_trace], 'prod_id': [prod_id]})
                df_all_trx = df_all_trx.append(all_trx)
                df_all_trx_grouped = df_all_trx.groupby(
                    [df_all_trx['order_date'], df_all_trx['prod_id']]).size().reset_index()

                # 2.Count Transaction which TD_Device Capture & Group By
                if line['同盾设备指纹'] != "" and line['同盾设备指纹'] != '111111':
                    td_cap_trx = pd.DataFrame(
                        {'order_date': [order_date], 'inst_trace': [inst_trace], 'prod_id': [prod_id]})
                    df_td_cap_trx = df_td_cap_trx.append(td_cap_trx)
                    df_td_cap_trx_grouped = df_td_cap_trx.groupby(
                        [df_td_cap_trx['order_date'], df_td_cap_trx['prod_id']]).size().reset_index()
                # 3.Count Transaction which ZY_Device Capture & Group By
                if line['自研设备指纹'] != "" and line['自研设备指纹'] != '000000':
                    zy_cap_trx = pd.DataFrame(
                        {'order_date': [order_date], 'inst_trace': [inst_trace], 'prod_id': [prod_id]})
                    df_zy_cap_trx = df_zy_cap_trx.append(zy_cap_trx)
                    df_zy_cap_trx_grouped = df_zy_cap_trx.groupby(
                        [df_zy_cap_trx['order_date'], df_zy_cap_trx['prod_id']]).size().reset_index()

    # 4.Merge All Trx & TD_Device Trx as merge_td
    merge_td = pd.merge(df_all_trx_grouped, df_td_cap_trx_grouped, how='left', on=['order_date', 'prod_id'])
    merge_td.columns = ['order_date', 'prod_id', 'trx_cnt', 'td_cap_cnt']

    # 5.Merge merge_td & ZY_Device Trx as merge_td_zy
    merge_td_zy = pd.merge(merge_td, df_zy_cap_trx_grouped, how='left', on=['order_date', 'prod_id'])
    merge_td_zy.columns = ['order_date', 'prod_id', 'trx_cnt', 'td_cap_cnt', 'zy_cap_cnt']

    # 6.Add tdCapRate, zyCapRate, batchNo in merge_td_zy as Final df
    merge_td_zy['td_cap_rate'] = merge_td_zy['td_cap_cnt'] / merge_td_zy['trx_cnt']
    merge_td_zy['zy_cap_rate'] = merge_td_zy['zy_cap_cnt'] / merge_td_zy['trx_cnt']
    merge_td_zy['batch_no'] = time.strftime("%Y%m%d%H%M%S")

    return merge_td_zy
