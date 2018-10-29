import pandas as pd
import time
import os
import sys
import csv


# ds_recg_rate


def get_ds_recg_rate(csv_reader):
    # print(os.path.basename(__file__), sys._getframe().f_code.co_name,
    #       sys._getframe().f_lineno, 'csv_reader = ', csv_reader)
    df_filtered_trx = pd.DataFrame({})
    df_td_cap_trx = pd.DataFrame({})
    df_zy_cap_trx = pd.DataFrame({})
    csv_reader1 = csv.reader(open(csv_reader))
    # print('csv_reader = ',csv_reader1)
    csv_read_result = csv.DictReader(open(csv_reader))
    for line in csv_read_result:
        if 1 < csv_read_result.line_num < 100:
            inst_trace = line['机构请求流水']
            order_date = line['商户订单日期']
            td_device = line['同盾设备指纹']
            zy_device = line['自研设备指纹']
            prod_id = line['支付产品编号'][0:8]

            # 1.Filter Transaction which has Both TD & ZY Device Fingerprint
            if td_device != "" and td_device != '111111' \
                    and zy_device != "" and zy_device != '000000':
                filtered_trx = pd.DataFrame({'order_date': [order_date], 'prod_id': [prod_id],
                                             'inst_trace': inst_trace,
                                             'td_device': [td_device], 'zy_device': [zy_device]})
                df_filtered_trx = df_filtered_trx.append(filtered_trx)

    df_filtered_trx = df_filtered_trx.reset_index()
    # print('df_filtered_trx\n', df_filtered_trx)
    df_filtered_trx_sort = df_filtered_trx.sort_values(by=['order_date'], ascending=[True])
    # df_filtered_trx_sort = df_filtered_trx_sort.reset_index()
    print('df_filtered_trx_sort\n', df_filtered_trx_sort)

    start_date = df_filtered_trx_sort.iloc[0]['order_date']
    end_date = df_filtered_trx_sort.iloc[-1]['order_date']


    rtn_prod_id = df_filtered_trx.iloc[0]['prod_id']

    rtn_date_range = start_date + '-' + end_date
    print('rtn_prod_id = ', rtn_prod_id)
    print('rtn_date_range = ',rtn_date_range)
    sys.exit(0)

    # 2.Get PRODUCT_ID
    rtn_prod_id = df_filtered_trx.loc[0, 'prod_id']

    # 3.Find Duplicated ZY_Device ==>> dup_zy_di
    df_filtered_trx_grouped = df_filtered_trx.groupby(
        [df_filtered_trx['prod_id'], df_filtered_trx['zy_device']]).size().reset_index()
    df_filtered_trx_grouped.columns = ['prod_id', 'zy_device', 'zyCapCnt']
    # print('df_filtered_trx_grouped\n', df_filtered_trx_grouped)
    dup_zy_di = df_filtered_trx_grouped[df_filtered_trx_grouped['zyCapCnt'] > 1]['zy_device'].reset_index()
    dup_zy_di.columns = ['index', 'dup_zy_device']

    # 4.Count Duplicated ZY_Device ==>> dup_zy_di_cnt
    dup_zy_di_cnt = len(dup_zy_di)

    # 5.Find TD_Device By Duplicated ZY_Device ==>> td_di
    td_di = pd.merge(dup_zy_di[['dup_zy_device']],
                     df_filtered_trx[['td_device', 'zy_device']],
                     how='inner',
                     left_on='dup_zy_device',
                     right_on='zy_device')
    # print('td_di\n', td_di)

    # 6.Count No-Duplicated TD_Device ==>> nodup_td_di_cnt
    nodup_td_di_cnt = len(td_di.drop_duplicates(['td_device']))

    # 6. Concatenate Return DataDataFrame ==>> rtn_df
    print('rtn_prod_id = ', rtn_prod_id)
    print('dup_zy_di_cnt = ', dup_zy_di_cnt)
    print('nodup_td_di_cnt = ', nodup_td_di_cnt)

    rtn_df = pd.DataFrame({'prod_id': [rtn_prod_id],
                           'dup_zy_di_cnt': [dup_zy_di_cnt],
                           'nodup_td_di_cnt': [nodup_td_di_cnt], }, index=None)

    print('rtn_df\n', rtn_df)


path = r"D:/github_program/myPython/docs/csvfiles/ds_recgrate/test.csv"  # P13F0000 1209-1207
#   payDate	        procID	    tdRecgCnt	zyRecgCnt	批次号
#   171209-171217	P13F0000	40	        34	        20171218101322

get_ds_recg_rate(path)
